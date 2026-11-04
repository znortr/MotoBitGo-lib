package core

import (
	"context"
	"errors"
	"fmt"
	stdnet "net"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/charliecharlieO-o/motobit-ride-go/hud/net"
	"github.com/charliecharlieO-o/motobit-ride-go/hud/stream"
	"github.com/charliecharlieO-o/motobit-ride-go/internal/logging"
	"github.com/google/uuid"
	"github.com/grandcat/zeroconf"
)

type HudEventSource int

const (
	UnknownEventSource HudEventSource = iota + 1
	EventSourcePXC
	EventSourceMediaControl
)

type HudEvent struct {
	Source HudEventSource
	Time   time.Time
	Cmd    int
	Data   any
}

type StoppableServer interface {
	Start() error
	Stop(ctx context.Context) error
}

type EcHost struct {
	Ip      string
	Port    string
	Package string
}

type CfmotoHUD struct {
	// State
	mu      sync.Mutex
	running bool

	// Config
	host        *EcHost
	targetFPS   int
	packageName string
	phoneUUID   uuid.UUID
	phoneConfig *net.PhoneConfig

	// Clock sync config
	clockTZID             string
	clockTZOffset         int
	earlyClockPushEnabled bool
	clockPushCount        int

	// net management
	keyPair      *net.KeyPair
	ecService    *net.ECService
	pxcControl   *net.PXCControl
	mediaControl *net.MediaControl
	mediaStream  *net.MediaStream

	// Streaming
	muxSource *stream.MuxSource

	// Communications
	stopped  chan any
	stopOnce sync.Once
	Events   chan HudEvent
	Errors   chan error
}

func NewCfmotoHUD(targetFPS int, mux *stream.MuxSource) *CfmotoHUD {
	if targetFPS <= 0 {
		targetFPS = 30
	}
	id := uuid.New()
	pkg := "com.cfmoto.cfmotointernational"
	return &CfmotoHUD{
		packageName: pkg,
		phoneUUID:   id,
		phoneConfig: &net.PhoneConfig{
			PxcVersion:            "1.0.2",
			PhoneUUID:             id.String(),
			PhoneBrand:            "google",
			PhoneModel:            "Pixel 6a",
			PhoneOsVersion:        "36",
			PhoneOs:               "Android",
			Package:               pkg,
			VersionCode:           121,
			Token:                 0,
			Pubkey:                "", // Done inside PXC
			EncryptedHUID:         "", // Done inside PXC
			BluetoothName:         "Pixel 6a",
			SupportH264IFrame:     true,
			AppVersionFingerPrint: "V:2.2.1(121)--ONLINE",
		},
		targetFPS: targetFPS,
		// Clock push: UNA sola consegna dell'ora per connessione (non il burst
		// storico 60x250ms che intasava la finestra TCP e causava le chiusure a
		// ~14 minuti, ne' l'assenza totale che riportava il display a 00.MM).
		// Il keepalive ogni 5 minuti (startKeepaliveClockPush) mantiene vivo
		// il canale; la risposta alla richiesta 0x10450 del TFT resta attiva.
		earlyClockPushEnabled: true,
		muxSource:             mux,
		stopped:               make(chan any),
		Events:                make(chan HudEvent, 32),
		Errors:                make(chan error, 32),
	}
}

func (hud *CfmotoHUD) handleServerEvent(evt any) {
	now := time.Now()
	hudEvent := HudEvent{Source: UnknownEventSource, Time: now, Data: evt}
	switch e := evt.(type) {
	case net.PXCResponse:
		if e.Command == net.PxcHeartbeat {
			break
		}
		hudEvent.Source = EventSourcePXC
		hudEvent.Cmd = int(e.Command)
		hudEvent.Data = e.Body
	case net.MediaCtrlResponse:
		hudEvent.Source = EventSourceMediaControl
		hudEvent.Cmd = int(e.Command)
		hudEvent.Data = e.Payload
	}

	select {
	case hud.Events <- hudEvent:
	default:
	}
}

func (hud *CfmotoHUD) isFatalErr(err error) bool {
	var fe net.FatalError
	if errors.As(err, &fe) {
		return fe.IsFatal()
	}
	return false
}

// nonFatalErr riporta all'app un errore come NON fatale. Serve a distinguere
// "la sessione e' persa" da "un canale e' caduto ma il resto funziona": l'app
// riconnette solo sugli errori fatali.
type nonFatalErr struct{ error }

func (nonFatalErr) IsFatal() bool { return false }

func (hud *CfmotoHUD) handleServerError(err error) {
	if err == nil {
		return
	}

	// Un guasto del solo canale di controllo (PXC :10922) NON deve abbattere la
	// sessione. Dai log: il PXC muore con READ_TIMEOUT mentre i frame su :10920
	// continuano a scorrere; era il nostro StopStream a chiudere gli altri canali
	// e a riportare il TFT alla schermata QR. Il listener su :10922 resta aperto,
	// quindi l'HU puo' riaprire il canale da solo.
	reported := err
	if _, isPxc := err.(*net.PxcError); isPxc && hud.IsRunning() {
		reported = nonFatalErr{err}
	}

	// Bubble up error (con la fatalita' corretta per l'app)
	select {
	case hud.Errors <- reported:
	default:
	}
	if hud.isFatalErr(reported) {
		// Fatal, we need to exit cleanly
		go func() {
			ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = hud.StopStream(ctxWithTimeout)
		}()
	}
}

func (hud *CfmotoHUD) startPxcEventFwd(s *net.PXCControl) {
	// PXC Events
	go func() {
		for evt := range s.Events {
			hud.handleServerEvent(evt)
		}
	}()
}

func (hud *CfmotoHUD) startMediaEventFwd(s *net.MediaControl) {
	// Media Ctrl events
	go func() {
		for evt := range s.Events {
			hud.handleServerEvent(evt)
		}
	}()
}

func (hud *CfmotoHUD) SearchForHost(ctx context.Context, timeout time.Duration) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	var mdns *net.MDNSService
	var entries <-chan *zeroconf.ServiceEntry

	if mdns, err = net.NewMDNSService(); err != nil {
		return err
	}
	if entries, err = mdns.LookupEntries(); err != nil {
		return err
	}

	go func() {
		mdnsErr := mdns.Browse(ctxWithTimeout, "_EasyConn._tcp", "local")
		if mdnsErr != nil {
			logging.Printf("error while browsing for mdns host: %s", mdnsErr)
		}
	}()

	// Find the display service IP, port and appropriate package name
	reService := regexp.MustCompile("^packagename=(" + hud.packageName + ")$")
	reIP := regexp.MustCompile("^ip=(.*)$")

	for {
		select {
		case <-ctxWithTimeout.Done():
			if errors.Is(ctxWithTimeout.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("search for host timed out after: %s", timeout)
			}
			return ctxWithTimeout.Err()
		case entry, ok := <-entries:
			if !ok {
				if ctxWithTimeout.Err() != nil {
					return ctxWithTimeout.Err()
				}
				return errors.New("mDNS entries channel closed before host found")
			}

			var ip, ecPort, packageName string

			for _, txt := range entry.Text {
				if reService.MatchString(txt) {
					sub := reService.FindStringSubmatch(txt)
					logging.Printf("Found EC service package: %s", sub[1])
					ecPort = strconv.Itoa(entry.Port)
					packageName = sub[1]
				}
				if reIP.MatchString(txt) {
					sub := reIP.FindStringSubmatch(txt)
					logging.Printf("Found EC IP package: %s", sub[1])
					ip = sub[1]
				}
			}

			if ip != "" && ecPort != "" {
				// Found what we need
				hud.host = &EcHost{
					Ip:      ip,
					Port:    ecPort,
					Package: packageName,
				}

				// Stop browsing as soon as we’ve found the host.
				cancel()

				return nil
			}
		}
	}
}

func (hud *CfmotoHUD) StartStream(ctx context.Context) (err error) {
	return hud.startStream(ctx, nil)
}

// StartStreamWithInitConn opens the reverse HUD servers before using a caller-routed EC handshake.
func (hud *CfmotoHUD) StartStreamWithInitConn(ctx context.Context, initConn stdnet.Conn) (err error) {
	return hud.startStream(ctx, initConn)
}

func (hud *CfmotoHUD) startStream(ctx context.Context, initConn stdnet.Conn) (err error) {
	hud.mu.Lock()
	if hud.running {
		hud.mu.Unlock()
		return errors.New("already running")
	}
	if hud.host == nil {
		hud.mu.Unlock()
		return errors.New("host is not set or has not been found")
	}
	if hud.muxSource == nil {
		hud.mu.Unlock()
		return errors.New("mux source is not set")
	}
	hud.mu.Unlock()

	if hud.keyPair, err = net.GenKeyPair(); err != nil {
		return err
	}

	var started []StoppableServer

	// If err != nil on exit, stop everything still running
	defer func() {
		if err == nil || len(started) == 0 {
			return
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// stop reverse order
		for i := len(started) - 1; i >= 0; i-- {
			if stopErr := started[i].Stop(shutdownCtx); stopErr != nil {
				logging.Printf("Error stopping server %T: %v", started[i], stopErr)
			}
		}

		hud.mu.Lock()
		hud.running = false
		hud.mu.Unlock()
	}()

	pxcReady := make(chan any, 1)
	pxcServer := net.NewPXCControl(":10922", hud.keyPair, hud.phoneConfig)
	hud.mu.Lock()
	tzID := hud.clockTZID
	tzOffset := hud.clockTZOffset
	earlyPush := hud.earlyClockPushEnabled
	pushCount := hud.clockPushCount
	hud.mu.Unlock()

	if tzID != "" {
		pxcServer.SetClockTimeZone(tzID, tzOffset)
	}
	pxcServer.SetEarlyClockPush(earlyPush)
	if pushCount > 0 {
		pxcServer.SetClockPushCount(pushCount)
	}

	// PXC is ready
	go func() {
		for event := range pxcServer.Events {
			if event.Command == net.PxcHeartbeat {
				// Notify PXC is ready non-blocking
				select {
				case pxcReady <- struct{}{}:
				default:
				}
				return // exit the routine
			}
		}
	}()
	// PXC error handling
	go func() {
		for pxcError := range pxcServer.Errors {
			hud.handleServerError(pxcError)
		}
	}()
	// NON inoltrare gli eventi PXC all'app.
	//
	// Verificato il 01/11: l'HU manda 0x80000010 a frequenza altissima (centinaia
	// di pacchetti al secondo). Inoltrandolo, ogni evento attraversa JNI, scrive
	// sul log e fa ridisegnare la UI: il main thread si satura, il mirroring si
	// blocca e la sessione va in stallo. Il canale PXC si serve qui, in Go: l'app
	// non ha bisogno di vederlo.
	// hud.startPxcEventFwd(pxcServer)

	mediaControl := net.NewMediaControl(":10921")
	mediaControl.OnVideoStart = hud.muxSource.PrepareLiveConsumer
	// -- maybe we could change chunkStep to something smaller to reduce latency?
	mediaStream := net.NewMediaStream(":10920", hud.muxSource, 0x1000, 3*time.Millisecond)

	// Media error handling
	go func() {
		for medCtrl := range mediaControl.Errors {
			hud.handleServerError(medCtrl)
		}
	}()
	go func() {
		for medStr := range mediaStream.Errors {
			hud.handleServerError(medStr)
		}
	}()
	hud.startMediaEventFwd(mediaControl)

	servers := []StoppableServer{pxcServer, mediaStream, mediaControl}
	started, err = startReverseServersThenInit(servers, func() error {
		logging.Printf("Reverse HUD servers are listening; sending EC stream init command")
		hud.ecService = net.NewECService(
			hud.host.Ip, hud.host.Port, hud.host.Package, hud.phoneConfig.PhoneOs,
		)
		if initConn != nil {
			return hud.ecService.InitStreamCmdWithConn(initConn)
		}
		return hud.ecService.InitStreamCmd()
	})
	if err != nil {
		hud.ecService = nil
		return err
	}

	// Wait until the T-Box has completed the reverse PXC handshake.
	select {
	case <-pxcReady:
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	hud.mu.Lock()
	hud.pxcControl = pxcServer
	hud.mediaStream = mediaStream
	hud.mediaControl = mediaControl
	hud.running = true
	hud.mu.Unlock()

	return nil
}

func startReverseServersThenInit(
	servers []StoppableServer,
	initStream func() error,
) (started []StoppableServer, err error) {
	for index, server := range servers {
		if err = server.Start(); err != nil {
			return started, fmt.Errorf("start reverse server %d (%T): %w", index, server, err)
		}
		started = append(started, server)
	}
	if err = initStream(); err != nil {
		return started, fmt.Errorf("initialize EasyConn stream: %w", err)
	}
	return started, nil
}

func (hud *CfmotoHUD) StopStream(ctx context.Context) error {
	hud.mu.Lock()
	if !hud.running {
		hud.mu.Unlock()
		hud.stopOnce.Do(func() {
			close(hud.stopped)
		})
		return nil
	}

	// Take copies of servers and clear state under lock
	pxc := hud.pxcControl
	mediaCtrl := hud.mediaControl
	mediaStream := hud.mediaStream

	hud.pxcControl = nil
	hud.mediaControl = nil
	hud.mediaStream = nil
	hud.running = false
	hud.mu.Unlock()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	servers := []StoppableServer{mediaStream, mediaCtrl, pxc}

	logging.Printf("Stopping stream servers")
	for _, server := range servers {
		if server == nil {
			continue
		}
		wg.Add(1)
		go func(server StoppableServer) {
			defer wg.Done()
			_ = server.Stop(ctxWithTimeout)
		}(server)
	}

	done := make(chan any)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	// close(hud.Events) -- treat them as broadcast channels, parent must close attached routines

	logging.Printf("signaling hud stopped")
	hud.stopOnce.Do(func() {
		close(hud.stopped)
	})

	return nil
}

// DiagnosticsSnapshot returns one low-frequency aggregate for each TCP channel.
func (hud *CfmotoHUD) DiagnosticsSnapshot() string {
	hud.mu.Lock()
	pxc := hud.pxcControl
	mediaCtrl := hud.mediaControl
	mediaStream := hud.mediaStream
	running := hud.running
	hud.mu.Unlock()

	snapshot := func(name string, diag *net.ChannelDiagnostics) string {
		if diag == nil {
			return name + "=unavailable"
		}
		return name + "{" + diag.Snapshot(time.Now(), diag.ActiveConnections()) + "}"
	}
	var pxcDiag, ctrlDiag, streamDiag *net.ChannelDiagnostics
	if pxc != nil {
		pxcDiag = pxc.ChannelDiagnostics()
	}
	if mediaCtrl != nil {
		ctrlDiag = mediaCtrl.ChannelDiagnostics()
	}
	if mediaStream != nil {
		streamDiag = mediaStream.ChannelDiagnostics()
	}

	// Campi che il TFT dichiara all'handshake (PxcHudConf): sono i suoi
	// parametri di sessione, tra cui il timeout socket su Wi-Fi sospettato
	// responsabile della chiusura dello streaming.
	confSummary := ""
	if pxc != nil && pxc.HudConfig != nil {
		confSummary = fmt.Sprintf(
			" conf{socketTimeoutPeriodWifi=%ds supportMirrorReconnect=%t carBrand=%q carModel=%q}",
			pxc.HudConfig.SocketTimeoutPeriodWifi,
			pxc.HudConfig.SupportMirrorReconnect,
			pxc.HudConfig.CarBrand, pxc.HudConfig.CarModel)
	}

	return fmt.Sprintf("running=%t targetFPS=%d %s %s %s%s", running, hud.targetFPS,
		snapshot("PXC", pxcDiag), snapshot("MediaControl", ctrlDiag), snapshot("MediaStream", streamDiag),
		confSummary)
}

func (hud *CfmotoHUD) IsRunning() bool {
	hud.mu.Lock()
	defer hud.mu.Unlock()
	return hud.running
}

func (hud *CfmotoHUD) Done() <-chan any {
	return hud.stopped
}

func (hud *CfmotoHUD) SetHost(host *EcHost) error {
	hud.mu.Lock()
	defer hud.mu.Unlock()
	if hud.running {
		return errors.New("already running")
	}
	hud.host = host
	return nil
}

func (hud *CfmotoHUD) SetClockTimeZone(id string, offsetSeconds int) {
	hud.mu.Lock()
	hud.clockTZID = id
	hud.clockTZOffset = offsetSeconds
	pxc := hud.pxcControl
	hud.mu.Unlock()

	if pxc != nil {
		pxc.SetClockTimeZone(id, offsetSeconds)
	}
}

func (hud *CfmotoHUD) SetEarlyClockPush(enabled bool) {
	hud.mu.Lock()
	hud.earlyClockPushEnabled = enabled
	pxc := hud.pxcControl
	hud.mu.Unlock()

	if pxc != nil {
		pxc.SetEarlyClockPush(enabled)
	}
}

// SetClockPushCount imposta quante volte la raffica iniziale consegna l'ora
// (vedi clocksync.go). Passato dall'app per la bisezione del numero minimo.
func (hud *CfmotoHUD) SetClockPushCount(count int) {
	hud.mu.Lock()
	hud.clockPushCount = count
	pxc := hud.pxcControl
	hud.mu.Unlock()

	if pxc != nil {
		pxc.SetClockPushCount(count)
	}
}

// SendSingleClockPush invia 1 singolo pacchetto PxcTimeSyncAck alla moto.
func (hud *CfmotoHUD) SendSingleClockPush() {
	hud.mu.Lock()
	pxc := hud.pxcControl
	hud.mu.Unlock()

	if pxc != nil {
		pxc.SendSingleClockPush()
	}
}
