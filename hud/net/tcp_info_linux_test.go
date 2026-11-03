//go:build linux || android

package net

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestReadTCPInfoOnRealSocket verifica che readTCPInfo funzioni su una
// socket TCP reale: nella 1.0.17 l'assertione all'interfaccia con firma
// sbagliata faceva fallire SEMPRE la lettura (tcp{infoUnavailable} sul
// campo) e nessun test se ne accorgeva perche' il fallback su host non
// Linux restituiva una stringa fissa senza eseguire il codice vero.
func TestReadTCPInfoOnRealSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback non disponibile: %v", err)
	}
	defer ln.Close()

	// Server che accetta e tiene la connessione aperta fino alla fine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := ln.Accept()
		if err != nil {
			return // listener chiuso dal test: uscita pulita
		}
		defer c.Close()
		buf := make([]byte, 1)
		// Read restituisce errore alla chiusura del lato client.
		_, _ = c.Read(buf)
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	info, err := readTCPInfo(c)
	if err != nil {
		c.Close()
		t.Fatalf("readTCPInfo su socket reale: %v", err)
	}
	if info == nil {
		c.Close()
		t.Fatal("readTCPInfo ha restituito nil senza errore")
	}
	if info.Ca_state != 1 { // TCP_CA_OPEN = 1 su connessione sana
		c.Close()
		t.Fatalf("ca_state inatteso: %d", info.Ca_state)
	}

	// Verifica anche il percorso del label: deve contenere i contatori.
	label := tcpInfoLabel(c)
	c.Close() // chiude prima di aspettare il server
	if !wgDone(&wg, 2*time.Second) {
		t.Log("server di test non terminato in tempo (ignorato)")
	}
	for _, want := range []string{"tcp{", "retrans=", "timer=", "ca="} {
		if !contains(label, want) {
			t.Fatalf("label %q privo di %q", label, want)
		}
	}
}

func wgDone(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
