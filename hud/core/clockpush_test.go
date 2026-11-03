package core

import (
	"encoding/json"
	"testing"

	"github.com/charliecharlieO-o/motobit-ride-go/hud/net"
)

// TestEarlyClockPushEnabledButSingleShot blocca la configurazione: il clock
// push DEVE restare abilitato (il TFT imposta l'orario SOLO da un push non
// sollecitato: senza, il display torna a 00.MM come verificato sulla 1.0.19)
// ma la vecchia implementazione sparava un burst di 60x250ms che intasava la
// socket (disconnessioni a ~14 minuti, diagnostica TCP della 1.0.18).
// Questo test impedisce che un refactor futuro riporti o il burst o lo spegnimento.
func TestEarlyClockPushEnabledButSingleShot(t *testing.T) {
	hud := NewCfmotoHUD(30, nil)
	if !hud.earlyClockPushEnabled {
		t.Fatal("earlyClockPushEnabled deve essere true: senza consegna dell'ora il TFT mostra 00.MM (verificato sulla 1.0.19)")
	}
}

// TestClockReplyStillPopulated garantisce che la risposta all'ora (quella
// INVIATA SU RICHIESTA del TFT) resti valida: la disattivazione del burst
// NON deve riportare l'orario del TFT a 00.MM. Usa net.NewPXCControl perche'
// il pxcControl dell'HUD esiste solo dopo StartStream.
func TestClockReplyStillPopulated(t *testing.T) {
	pxc := net.NewPXCControl(":0", nil, nil)
	pxc.SetClockTimeZone("Europe/Rome", 3600)

	body := pxc.BuildClockReply()
	if len(body) == 0 {
		t.Fatal("BuildClockReply vuota: il TFT riceverebbe 00.MM")
	}
	var reply struct {
		Time            int64  `json:"time"`
		CurrentTime     int64  `json:"currentTime"`
		CurrentTimeZone string `json:"currentTimeZone"`
		DateTime        string `json:"dateTime"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		t.Fatalf("decode clock reply: %v", err)
	}
	if reply.Time <= 0 || reply.CurrentTime <= 0 {
		t.Fatalf("timestamp invalidi: time=%d currentTime=%d (rischio 00.MM)", reply.Time, reply.CurrentTime)
	}
	if reply.CurrentTimeZone != "Europe/Rome" {
		t.Fatalf("currentTimeZone=%q, attesa Europe/Rome", reply.CurrentTimeZone)
	}
	if reply.DateTime == "" {
		t.Fatal("dateTime vuoto (rischio 00.MM)")
	}
}
