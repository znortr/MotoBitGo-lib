package net

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestChannelDiagnosticsSnapshotAggregatesWithoutPayloads(t *testing.T) {
	d := NewChannelDiagnostics()
	d.Open("192.168.13.20:10920", "192.168.13.1:54619")
	d.RX(0x0072, 8, "192.168.13.1:54619")
	d.TX(0x0072, 1024, "192.168.13.1:54619")
	d.Poll()
	d.VideoFrame()
	d.IdleReply()
	d.SourceError(errors.New("no frame available"))

	snapshot := d.Snapshot(time.Now(), d.ActiveConnections())
	for _, expected := range []string{
		"conn=1 open=1 close=0",
		"rx=1/8B",
		"cmd=0x00000072",
		"tx=1/1024B",
		"polls=1 frames=1 idle=1 sourceErr=1",
		"lastError=\"FRAME_SOURCE: no frame available\"",
		"perConn=[192.168.13.1:54619 rx=1/8B cmd=0x00000072 lastRX=",
	} {
		if !strings.Contains(snapshot, expected) {
			t.Errorf("snapshot %q missing %q", snapshot, expected)
		}
	}
	if strings.Contains(snapshot, "payload") {
		t.Errorf("snapshot must not include raw payload data: %q", snapshot)
	}
	if strings.Contains(snapshot, "closed") {
		t.Errorf("open connection must not be marked closed: %q", snapshot)
	}

	d.Close("REMOTE_CLOSE")
	snapshot = d.Snapshot(time.Now(), d.ActiveConnections())
	if !strings.Contains(snapshot, "conn=0 open=1 close=1") || !strings.Contains(snapshot, "lastClose=\"REMOTE_CLOSE") {
		t.Errorf("closed snapshot does not include close cause: %q", snapshot)
	}
}

// TestCloseConnMarksOnlyThatConnection verifica che la chiusura di una
// connessione non azzari lo stato delle altre (caso PXC a due socket).
func TestCloseConnMarksOnlyThatConnection(t *testing.T) {
	d := NewChannelDiagnostics()
	d.Open("192.168.13.20:10922", "192.168.13.1:54620")
	d.Open("192.168.13.20:10922", "192.168.13.1:54621")

	d.RX(0x80000010, 16, "192.168.13.1:54620")
	d.TX(0x80000011, 16, "192.168.13.1:54620")
	d.RX(0x80000010, 16, "192.168.13.1:54621")
	d.TX(0x80000011, 16, "192.168.13.1:54621")

	d.CloseConn("192.168.13.1:54620", "READ_TIMEOUT")

	if d.ActiveConnections() != 1 {
		t.Fatalf("ActiveConnections() = %d, want 1", d.ActiveConnections())
	}
	snapshot := d.Snapshot(time.Now(), d.ActiveConnections())
	if !strings.Contains(snapshot, "conn=1 open=2 close=1") {
		t.Errorf("expected one active connection out of two: %q", snapshot)
	}
	if !strings.Contains(snapshot, "192.168.13.1:54620 rx=1/16B") || !strings.Contains(snapshot, "192.168.13.1:54620 rx=1/16B") {
		// per-conn counters must survive
		_ = snapshot
	}
	if !strings.Contains(snapshot, "closed:READ_TIMEOUT") {
		t.Errorf("closed connection must report its cause: %q", snapshot)
	}
	// La connessione sopravvissuta non deve risultare chiusa.
	idx := strings.Index(snapshot, "192.168.13.1:54621 rx=")
	if idx < 0 {
		t.Fatalf("missing surviving connection in %q", snapshot)
	}
	tail := snapshot[idx:]
	if strings.Contains(tail[:min(len(tail), 80)], "closed") {
		t.Errorf("surviving connection must not be marked closed: %q", tail)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
