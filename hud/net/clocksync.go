package net

import (
	"encoding/json"
	"time"

	"github.com/charliecharlieO-o/motobit-ride-go/internal/logging"
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

func (s *PXCControl) startEarlyClockPush() {
	s.clockMu.Lock()
	enabled := s.earlyPushEnabled
	s.clockMu.Unlock()

	if !enabled {
		return
	}

	s.earlyPushOnce.Do(func() {
		go func() {
			logging.Printf("Starting early clock push loop (60 iterations @ 250ms)")
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()

			for i := 0; i < 60; i++ {
				conns := s.tracker.Snapshot()
				if len(conns) == 0 {
					logging.Printf("Early clock push stopping: no active TCP connections")
					return
				}

				body := s.BuildClockReply()
				resp := &PXCResponse{
					Command: PxcTimeSyncAck,
					Body:    body,
				}

				if i%8 == 0 {
					logging.Printf("Early clock push iteration %d/60 sending PxcTimeSyncAck (0x10451) to %d connection(s)", i, len(conns))
				}

				for _, conn := range conns {
					if err := s.writeResponse(resp, conn, nil); err != nil {
						logging.Printf("Error sending early clock push to %s: %v", conn.RemoteAddr(), err)
					}
				}

				if i < 59 {
					select {
					case <-s.quit:
						logging.Printf("Early clock push exiting: PXC control quit")
						return
					case <-ticker.C:
					}
				}
			}
			logging.Printf("Early clock push loop completed")
		}()
	})
}
