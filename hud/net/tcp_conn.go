package net

import (
	"net"
	"syscall"
)

// syscallConnOf estrae la syscall.RawConn da una net.Conn TCP. E' il tipo
// esatto restituito da net.TCPConn.SyscallConn(): usare un'interfaccia
// propria con la stessa firma NON soddisfa l'assertione (i tipi dei metodi
// devono combaciare esattamente), e faceva fallire sempre readTCPInfo
// (visto sul campo come tcp{infoUnavailable} nella 1.0.17).
func syscallConnOf(conn net.Conn) (syscall.RawConn, error) {
	sc, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return nil, errNotSyscallConn
	}
	return sc.SyscallConn()
}

// notSyscallConnError segnala una connessione che non espone la RawConn.
type notSyscallConnError struct{}

func (notSyscallConnError) Error() string { return "conn does not expose syscall conn" }

// errNotSyscallConn e' l'istanza singleton dell'errore.
var errNotSyscallConn error = notSyscallConnError{}
