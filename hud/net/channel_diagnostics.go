package net

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// connDiag e' lo stato di una singola connessione TCP: contatori RX/TX e
// timestamp/ultimo comando. Serve a capire, nel riepilogo, su quale delle
// connessioni del canale viaggia davvero il traffico e quale e' morta.
type connDiag struct {
	local      string
	remote     string
	rx         uint64
	rxBytes    uint64
	tx         uint64
	txBytes    uint64
	lastRx     time.Time
	lastTx     time.Time
	lastRxCmd  uint32
	lastTxCmd  uint32
	closed     bool
	closeAt    time.Time
	closeCause string

	// conn e' la socket TCP reale, tenuta per leggere i contatori del
	// kernel (TCP_INFO: retransmissions, RTO, backoff) nel Snapshot.
	conn net.Conn
}

// ChannelDiagnostics contiene solo contatori aggregati e, per ogni
// connessione del canale, contatori e timestamp: non conserva pacchetti o
// payload e la memoria non cresce con il traffico.
type ChannelDiagnostics struct {
	mu sync.Mutex

	started time.Time
	active  int
	opened  uint64
	closed  uint64

	rxFrames uint64
	rxBytes  uint64
	txFrames uint64
	txBytes  uint64

	lastRX     time.Time
	lastRXCmd  uint32
	lastRXPeer string
	lastTX     time.Time
	lastTXCmd  uint32
	lastTXPeer string

	lastCloseAt time.Time
	lastClose   string
	lastErrorAt time.Time
	lastError   string

	polls        uint64
	videoFrames  uint64
	idleReplies  uint64
	sourceErrors uint64

	conns map[string]*connDiag
}

func NewChannelDiagnostics() *ChannelDiagnostics {
	return &ChannelDiagnostics{
		started: time.Now(),
		conns:   make(map[string]*connDiag),
	}
}

func (d *ChannelDiagnostics) ActiveConnections() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}

func (d *ChannelDiagnostics) conn(peer string) *connDiag {
	c, ok := d.conns[peer]
	if !ok {
		c = &connDiag{remote: peer}
		d.conns[peer] = c
	}
	return c
}

func (d *ChannelDiagnostics) Open(local, remote string) {
	d.OpenConn(local, remote, nil)
}

// OpenConn registra una nuova connessione tenendo anche la socket TCP, cosi'
// il Snapshot puo' leggere i contatori del kernel (TCP_INFO: retransmissions,
// RTO, backoff) direttamente su di essa via getsockopt.
func (d *ChannelDiagnostics) OpenConn(local, remote string, conn net.Conn) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.active++
	d.opened++
	// Ogni connessione usa una porta remota diversa: la voce precedente
	// con la stessa chiave (eventuale) viene sostituita.
	d.conns[remote] = &connDiag{local: local, remote: remote, conn: conn}
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) Close(reason string) {
	if d == nil {
		return
	}
	now := time.Now()
	d.mu.Lock()
	if d.active > 0 {
		d.active--
	}
	d.closed++
	d.lastCloseAt = now
	d.lastClose = fmt.Sprintf("%s afterLastRX=%s afterLastTX=%s", reason,
		sinceString(now, d.lastRX), sinceString(now, d.lastTX))
	d.mu.Unlock()
}

// CloseConn chiude una specifica connessione (identificata dalla porta remota)
// e la marca come chiusa nel dettaglio per-connessione.
func (d *ChannelDiagnostics) CloseConn(remote, reason string) {
	if d == nil {
		return
	}
	now := time.Now()
	d.mu.Lock()
	if d.active > 0 {
		d.active--
	}
	d.closed++
	d.lastCloseAt = now
	d.lastClose = fmt.Sprintf("%s afterLastRX=%s afterLastTX=%s", reason,
		sinceString(now, d.lastRX), sinceString(now, d.lastTX))
	if c, ok := d.conns[remote]; ok && !c.closed {
		c.closed = true
		c.closeAt = now
		c.closeCause = reason
	}
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) RX(cmd uint32, size uint64, peer string) {
	if d == nil {
		return
	}
	now := time.Now()
	d.mu.Lock()
	d.rxFrames++
	d.rxBytes += size
	d.lastRX = now
	d.lastRXCmd = cmd
	d.lastRXPeer = peer
	c := d.conn(peer)
	c.rx++
	c.rxBytes += size
	c.lastRx = now
	c.lastRxCmd = cmd
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) TX(cmd uint32, size uint64, peer string) {
	if d == nil {
		return
	}
	now := time.Now()
	d.mu.Lock()
	d.txFrames++
	d.txBytes += size
	d.lastTX = now
	d.lastTXCmd = cmd
	d.lastTXPeer = peer
	c := d.conn(peer)
	c.tx++
	c.txBytes += size
	c.lastTx = now
	c.lastTxCmd = cmd
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) Error(kind string, err error) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.lastErrorAt = time.Now()
	if err != nil {
		d.lastError = fmt.Sprintf("%s: %s", kind, strings.TrimSpace(err.Error()))
	} else {
		d.lastError = kind
	}
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) Poll() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.polls++
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) VideoFrame() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.videoFrames++
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) IdleReply() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.idleReplies++
	d.mu.Unlock()
}

func (d *ChannelDiagnostics) SourceError(err error) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.sourceErrors++
	d.lastErrorAt = time.Now()
	if err != nil {
		d.lastError = fmt.Sprintf("FRAME_SOURCE: %s", strings.TrimSpace(err.Error()))
	} else {
		d.lastError = "FRAME_SOURCE"
	}
	d.mu.Unlock()
}

// Snapshot usa time.Time monotono per le eta': i cambi dell'ora civile non
// falsano gli intervalli. Nessun pacchetto viene esposto.
func (d *ChannelDiagnostics) Snapshot(now time.Time, activeConnections int) string {
	if d == nil {
		return "unavailable"
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	summary := fmt.Sprintf(
		"up=%s conn=%d open=%d close=%d rx=%d/%dB lastRX=%s cmd=0x%08x rxPeer=%s tx=%d/%dB lastTX=%s cmd=0x%08x txPeer=%s polls=%d frames=%d idle=%d sourceErr=%d lastCloseAge=%s lastClose=%q lastErrorAge=%s lastError=%q",
		now.Sub(d.started).Round(time.Second), activeConnections, d.opened, d.closed,
		d.rxFrames, d.rxBytes, sinceString(now, d.lastRX), d.lastRXCmd, d.lastRXPeer,
		d.txFrames, d.txBytes, sinceString(now, d.lastTX), d.lastTXCmd, d.lastTXPeer,
		d.polls, d.videoFrames, d.idleReplies, d.sourceErrors,
		sinceString(now, d.lastCloseAt), d.lastClose, sinceString(now, d.lastErrorAt), d.lastError,
	)

	var b strings.Builder
	b.WriteString(summary)
	if len(d.conns) > 0 {
		b.WriteString(" perConn=[")
		first := true
		for _, c := range d.conns {
			if !first {
				b.WriteString("; ")
			}
			first = false
			state := "alive"
			if c.closed {
				state = fmt.Sprintf("closed:%s@%s", c.closeCause, sinceString(now, c.closeAt))
			}
			tcp := ""
			if !c.closed && c.conn != nil {
				// Solo se la socket e' ancora aperta: dopo la chiusura i
				// contatori non sono piu' leggibili.
				tcp = tcpInfoLabel(c.conn)
			}
			fmt.Fprintf(&b, "%s rx=%d/%dB cmd=0x%08x lastRX=%s tx=%d/%dB cmd=0x%08x lastTX=%s %s%s",
				c.remote, c.rx, c.rxBytes, c.lastRxCmd, sinceString(now, c.lastRx),
				c.tx, c.txBytes, c.lastTxCmd, sinceString(now, c.lastTx), state,
				tcp)
		}
		b.WriteString("]")
	}
	return b.String()
}

func sinceString(now, then time.Time) string {
	if then.IsZero() {
		return "never"
	}
	return now.Sub(then).Round(time.Millisecond).String()
}
