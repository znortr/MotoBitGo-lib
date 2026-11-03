//go:build linux || android

package net

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// tcpInfoLabel restituisce, per la connessione indicata, un'etichetta con i
// contatori TCP letti dal kernel direttamente sulla socket tramite
// getsockopt(TCP_INFO). Non richiede /proc (che su Android e' tipicamente
// inaccessibile alle app, come verificato sul campo) ne' permessi aggiuntivi.
//
// Serve a stabilire QUANDO il percorso verso il TFT inizia a perdersi:
//   - Retrans che sale mentre la connessione e' ancora "alive" -> il kernel
//     sta ritrasmettendo perche' il TFT non ACKa: il guasto e' in corso;
//   - Retrans fermo fino al READ_TIMEOUT                          -> il guasto e'
//     istantaneo.
//
// Su conn non-TCP o in errore restituisce " tcp{infoUnavailable}".
func tcpInfoLabel(conn net.Conn) string {
	info, err := readTCPInfo(conn)
	if err != nil || info == nil {
		return " tcp{infoUnavailable}"
	}
	timer := "off"
	if info.Backoff > 0 {
		timer = fmt.Sprintf("active(rto=%dms backoff=%d)", info.Rto/1000, info.Backoff)
	}
	return fmt.Sprintf(" tcp{retrans=%d timer=%s ca=%d}", info.Retrans, timer, info.Ca_state)
}

// readTCPInfo estrae TCP_INFO dalla socket via getsockopt.
func readTCPInfo(conn net.Conn) (*unix.TCPInfo, error) {
	raw, err := syscallConnOf(conn)
	if err != nil {
		return nil, err
	}
	var info *unix.TCPInfo
	var opErr error
	err = raw.Control(func(fd uintptr) {
		// Control passa l'fd come uintptr: unix.GetsockoptTCPInfo vuole int.
		info, opErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if err == nil {
		err = opErr
	}
	return info, err
}
