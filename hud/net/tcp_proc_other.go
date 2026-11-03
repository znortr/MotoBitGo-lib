//go:build !linux && !android

package net

import "net"

// tcpInfoLabel su sistemi non-Linux (es. compilazione dei test su Windows):
// TCP_INFO via getsockopt non e' disponibile; sul device Android (GOOS
// android/linux) viene usata la versione reale in tcp_proc_linux.go.
func tcpInfoLabel(net.Conn) string {
	return " tcp{infoUnsupportedOnHost}"
}
