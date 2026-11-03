package net

import (
	"net"
	"sync"
)

type ConnTracker struct {
	mu          sync.Mutex
	connections map[net.Conn]struct{}
}

func NewConnTracker() *ConnTracker {
	return &ConnTracker{
		connections: make(map[net.Conn]struct{}),
	}
}

func (t *ConnTracker) Add(conn net.Conn) {
	t.mu.Lock()
	t.connections[conn] = struct{}{}
	t.mu.Unlock()
}

func (t *ConnTracker) Remove(conn net.Conn) {
	t.mu.Lock()
	delete(t.connections, conn)
	t.mu.Unlock()
}

func (t *ConnTracker) Snapshot() []net.Conn {
	t.mu.Lock()
	defer t.mu.Unlock()

	conns := make([]net.Conn, 0, len(t.connections))
	for c := range t.connections {
		conns = append(conns, c)
	}
	return conns
}

func (t *ConnTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.connections)
}

func (t *ConnTracker) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for c := range t.connections {
		_ = c.Close()
	}
}
