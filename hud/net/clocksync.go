package net

import (
	"encoding/json"
	"net"
	"time"

	"github.com/charliecharlieO-o/motobit-ride-go/internal/logging"
)

// keepaliveClockPushPeriod e' molto inferiore al timeout del watchdog del
// TFT osservato nei log (~14 minuti): un invio ogni 5 minuti resetta il timer.
const keepaliveClockPushPeriod = 5 * time.Minute

// Parametri della consegna dell'ora (vedi sendClockPushOnConnect).
// Il TFT imposta l'orario SOLO da una raffica di push non richiesti
// (PxcTimeSyncAck) nei primi secondi dopo l'apertura della connessione:
// senza push il display resta a 00.MM (verificato sulla 1.0.19), un push
// singolo non basta (1.0.20) e neppure 30 push diluiti ogni 2s (1.0.21);
// la raffica storica di 60x250ms funzionava MA intasava la finestra TCP
// della socket (diagnostica 1.0.18: backoff gia' a 31s, chiusura ~14 min).
// Il numero di push e' quindi CONFIGURABILE (SetClockPushCount) per
// individuare per bisezione la dose minima che il TFT accetta.
const (
	// Intervallo tra i push della raffica iniziale (come nel commit
	// 1f742b0, che impostava correttamente l'ora).
	clockPushBurstPeriod = 250 * time.Millisecond
	// Default conservativo: il valore noto-good della 1.0.18.
	defaultClockPushCount = 60
	clockMaxPushCount     = 120
)

type clockReply struct {
	Time            int64  `json:"time"`
	CurrentTime     int64  `json:"currentTime"`
	CurrentTimeZone string `json:"currentTimeZone"`
	DateTime        string `json:"dateTime,omitempty"`
}

func (s *PXCControl) BuildClockReply() []byte {
	s.clockMu.Lock()
	tzID := s.clockTZID
	offsetSec := s.clockTZOffset
	s.clockMu.Unlock()

	if tzID == "" {
		tzID = "Europe/Rome"
	}

	now := time.Now().UTC()
	gmtMs := now.UnixMilli()
	currentTimeMs := gmtMs + int64(offsetSec)*1000

	localTime := now.Add(time.Duration(offsetSec) * time.Second)
	dateTimeStr := localTime.Format("02.01.2006 15:04:05.000")

	reply := clockReply{
		Time:            gmtMs,
		CurrentTime:     currentTimeMs,
		CurrentTimeZone: tzID,
		DateTime:        dateTimeStr,
	}

	b, err := json.Marshal(reply)
	if err != nil {
		logging.Printf("Error marshaling clock reply: %v", err)
		return nil
	}
	return b
}

func (s *PXCControl) SetClockTimeZone(id string, offsetSeconds int) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	s.clockTZID = id
	s.clockTZOffset = offsetSeconds
}

func (s *PXCControl) SetEarlyClockPush(enabled bool) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	s.earlyPushEnabled = enabled
}

// startKeepaliveClockPush invia PxcTimeSyncAck su tutte le connessioni PXC
// attive ogni 5 minuti, per tutta la vita del controllo. Il watchdog del TFT
// chiude la sessione (~14 minuti) se non riceve nulla dal telefono sulla
// connessione heartbeat; il push periodico evita che il timer scatti.
// Il traffico e' un pacchetto da ~120B ogni 300 s: trascurabile.
func (s *PXCControl) startKeepaliveClockPush() {
	go func() {
		ticker := time.NewTicker(keepaliveClockPushPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-s.quit:
				return
			case <-ticker.C:
			}

			hConn := s.getHeartbeatConn()
			if hConn == nil {
				continue
			}

			resp := &PXCResponse{
				Command: PxcTimeSyncAck,
				Body:    s.BuildClockReply(),
			}

			if err := s.writeResponse(resp, hConn, nil); err != nil {
				logging.Printf("Keepalive clock push error to %s: %v", hConn.RemoteAddr(), err)
			}
		}
	}()
}

// SetClockPushCount imposta quante volte consegna l'ora all'apertura di ogni
// connessione (raffica a clockPushBurstPeriod). Configurabile per test.
func (s *PXCControl) SetClockPushCount(count int) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if count < 0 {
		count = 0
	}
	if count > clockMaxPushCount {
		count = clockMaxPushCount
	}
	s.clockPushCount = count
}

// sendClockPushOnConnect consegna l'ora al TFT con una raffica di push non
// richiesti (PxcTimeSyncAck) a intervalli di clockPushBurstPeriod, per
// clockPushCount volte, appena aperta la connessione. Avviato da handleConn.
// sendClockPushOnConn consegna l'ora al TFT con una raffica di push non
// richiesti (PxcTimeSyncAck) a intervalli di clockPushBurstPeriod, per
// clockPushCount volte, sulla connessione di handshake iniziale (conn).
func (s *PXCControl) sendClockPushOnConn(conn net.Conn) {
	s.clockMu.Lock()
	enabled := s.earlyPushEnabled
	count := s.clockPushCount
	s.clockMu.Unlock()

	// Se count == 1, l'utente ha scelto l'invio sincronizzato al primo frame:
	// la raffica iniziale dell'handshake viene saltata.
	if !enabled || count <= 1 || conn == nil {
		return
	}

	s.earlyPushOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(clockPushBurstPeriod)
			defer ticker.Stop()

			for i := 0; i < count; i++ {
				if i > 0 {
					select {
					case <-s.quit:
						return
					case <-ticker.C:
					}
				}

				resp := &PXCResponse{
					Command: PxcTimeSyncAck,
					Body:    s.BuildClockReply(),
				}
				if err := s.writeResponse(resp, conn, nil); err != nil {
					logging.Printf("Clock push %d/%d to %s: %v", i+1, count, conn.RemoteAddr(), err)
				}
			}
			logging.Printf("Clock push raffica completata: %d push su socket %s", count, conn.RemoteAddr())
		}()
	})
}

// SendSingleClockPush invia 1 singolo pacchetto PxcTimeSyncAck alla moto,
// sincronizzato con l'avvio del primo frame video.
func (s *PXCControl) SendSingleClockPush() {
	s.clockMu.Lock()
	hConn := s.heartbeatConn
	s.clockMu.Unlock()

	if hConn == nil {
		conns := s.tracker.Snapshot()
		if len(conns) > 0 {
			hConn = conns[0]
		}
	}

	if hConn == nil {
		logging.Printf("SendSingleClockPush: nessuna connessione PXC disponibile")
		return
	}

	resp := &PXCResponse{
		Command: PxcTimeSyncAck,
		Body:    s.BuildClockReply(),
	}

	if err := s.writeResponse(resp, hConn, nil); err != nil {
		logging.Printf("SendSingleClockPush error to %s: %v", hConn.RemoteAddr(), err)
	} else {
		logging.Printf("SendSingleClockPush (1 pacchetto al primo frame) inviato con successo a %s", hConn.RemoteAddr())
	}
}
