package net

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	stdnet "net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPXCDiagnosticsSnapshotIncludesErrorsAndConnections(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	control.diag.Open("192.168.13.20:10922", "192.168.13.1:54612")
	control.recordDiagRx(PxcHeartbeat, 16, "192.168.13.1:54612")
	control.recordDiagTx(PxcHeartbeatOk, 16, "192.168.13.1:54612")
	control.recordDiagError("READ_TIMEOUT", errors.New("read timed out"))

	snapshot := control.DiagnosticsSnapshot()
	for _, expected := range []string{
		"conn=1 open=1 close=0",
		"rx=1/16B",
		"tx=1/16B",
		"cmd=0x70000000",
		"cmd=0x70000001",
		"lastError=\"READ_TIMEOUT: read timed out\"",
	} {
		if !strings.Contains(snapshot, expected) {
			t.Errorf("snapshot %q missing %q", snapshot, expected)
		}
	}
}

func TestUnknownEvenPXCRequestWithBodyIsAcknowledged(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	responses := handlePXCEventAndReadResponses(t, control, &PXCResponse{
		Command: PxcOtaFtpInfo,
		Body:    json.RawMessage(`{"port":11021}`),
	}, 1)

	if responses[0].Command != PxcOtaFtpInfo+1 {
		t.Fatalf("ACK command = 0x%x, want 0x%x", responses[0].Command, PxcOtaFtpInfo+1)
	}
	if len(responses[0].Body) != 0 {
		t.Fatalf("ACK body length = %d, want 0", len(responses[0].Body))
	}
	assertNoPXCError(t, control)
}

func TestCheckSnSendsAckAndPositiveResult(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	responses := handlePXCEventAndReadResponses(t, control, &PXCResponse{
		Command: PxcClientSet,
		Body:    json.RawMessage(`{"client_set":"easy_conn","sn":"SERIAL-123"}`),
	}, 2)

	if responses[0].Command != PxcCheckSnAck {
		t.Fatalf("CHECK_SN ACK command = 0x%x, want 0x%x", responses[0].Command, PxcCheckSnAck)
	}
	if responses[1].Command != PxcCheckSnResult {
		t.Fatalf("CHECK_SN result command = 0x%x, want 0x%x", responses[1].Command, PxcCheckSnResult)
	}
	var result checkSnResult
	if err := json.Unmarshal(responses[1].Body, &result); err != nil {
		t.Fatalf("decode CHECK_SN result: %v", err)
	}
	if !result.IsOK || result.ID != "SERIAL-123" || result.ClientSet != "easy_conn" {
		t.Fatalf("unexpected CHECK_SN result: %+v", result)
	}
	assertNoPXCError(t, control)
}

func TestUnknownOddPXCResponseIsNotAcknowledged(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	if err := server.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	control.handleEvent(&PXCResponse{Command: PxcMediaFeatureConf + 1}, client)
	buffer := make([]byte, 1)
	if _, err := server.Read(buffer); err == nil {
		t.Fatal("unknown odd PXC response unexpectedly produced an ACK")
	}
	assertNoPXCError(t, control)
}

func TestPxcTimeSyncResponse(t *testing.T) {
	control := NewPXCControl(":0", nil, nil)
	control.SetClockTimeZone("Europe/Rome", 7200)

	responses := handlePXCEventAndReadResponses(t, control, &PXCResponse{
		Command: PxcTimeSync,
	}, 1)

	if responses[0].Command != PxcTimeSyncAck {
		t.Fatalf("PXC_TIME_SYNC ACK command = 0x%x, want 0x%x", responses[0].Command, PxcTimeSyncAck)
	}

	var reply clockReply
	if err := json.Unmarshal(responses[0].Body, &reply); err != nil {
		t.Fatalf("decode PXC_TIME_SYNC reply: %v", err)
	}

	if reply.CurrentTimeZone != "Europe/Rome" {
		t.Fatalf("unexpected currentTimeZone: %s", reply.CurrentTimeZone)
	}
	if reply.Time <= 0 || reply.CurrentTime <= 0 {
		t.Fatalf("invalid timestamps: time=%d currentTime=%d", reply.Time, reply.CurrentTime)
	}
	if reply.DateTime == "" {
		t.Fatal("dateTime is empty")
	}
	assertNoPXCError(t, control)
}

// TestPXCConcurrentWritesDoNotInterleave verifica che due goroutine che
// rispondono sulla STESSA connessione non corrompano i pacchetti.
// Con il vecchio codice (una Write per campo dell'header + una per il corpo) i
// pezzi si intercalavano e il magic check falliva: era la causa sospetta delle
// disconnessioni durante il clock push.
func TestPXCConcurrentWritesDoNotInterleave(t *testing.T) {
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()

	control := NewPXCControl(":0", nil, nil)
	body := []byte("0123456789ABCDEF")
	const perWriter = 300

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < perWriter; i++ {
			_ = control.writeResponse(&PXCResponse{Command: PxcHandshakeOk, Body: body}, client, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < perWriter; i++ {
			_ = control.writeResponse(&PXCResponse{Command: PxcTimeSyncAck, Body: body}, client, nil)
		}
	}()

	reader := bufio.NewReader(server)
	for read := 0; read < 2*perWriter; read++ {
		header := make([]byte, pxcHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			t.Fatalf("lettura header %d: %v", read, err)
		}
		cmd := binary.LittleEndian.Uint32(header[0:4])
		size := binary.LittleEndian.Uint32(header[4:8])
		magic := binary.LittleEndian.Uint32(header[8:12])

		if size != uint32(pxcHeaderSize+len(body)) {
			t.Fatalf("pacchetto %d: size=%d (atteso %d)", read, size, pxcHeaderSize+len(body))
		}
		if magic != size^cmd {
			t.Fatalf("pacchetto %d CORROTTO: magic=0x%x atteso 0x%x", read, magic, size^cmd)
		}

		payload := make([]byte, size-uint32(pxcHeaderSize))
		if _, err := io.ReadFull(reader, payload); err != nil {
			t.Fatalf("lettura body %d: %v", read, err)
		}
		if !bytes.Equal(payload, body) {
			t.Fatalf("pacchetto %d: body corrotto %q", read, payload)
		}
	}

	wg.Wait()
}

func handlePXCEventAndReadResponses(
	t *testing.T,
	control *PXCControl,
	event *PXCResponse,
	count int,
) []PXCResponse {
	t.Helper()
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		control.handleEvent(event, client)
		close(done)
	}()

	responses := make([]PXCResponse, 0, count)
	for i := 0; i < count; i++ {
		header := make([]byte, pxcHeaderSize)
		if _, err := io.ReadFull(server, header); err != nil {
			t.Fatalf("read PXC response header %d: %v", i, err)
		}
		response := PXCResponse{
			Command: binary.LittleEndian.Uint32(header[0:4]),
			Size:    binary.LittleEndian.Uint32(header[4:8]),
			Magic:   binary.LittleEndian.Uint32(header[8:12]),
			Token:   binary.LittleEndian.Uint32(header[12:16]),
		}
		if response.Size < pxcHeaderSize {
			t.Fatalf("invalid PXC response size %d", response.Size)
		}
		if response.Magic != response.Size^response.Command {
			t.Fatalf("invalid PXC response magic 0x%x", response.Magic)
		}
		body := make([]byte, int(response.Size)-pxcHeaderSize)
		if _, err := io.ReadFull(server, body); err != nil {
			t.Fatalf("read PXC response body %d: %v", i, err)
		}
		response.Body = body
		responses = append(responses, response)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PXC handler did not complete")
	}
	return responses
}

func assertNoPXCError(t *testing.T, control *PXCControl) {
	t.Helper()
	select {
	case err := <-control.Errors:
		t.Fatalf("unexpected PXC error: %v", err)
	default:
	}
}
