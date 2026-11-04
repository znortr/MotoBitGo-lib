//go:build windows

package net

import (
	"context"
	"net"
	"syscall"
)

func listenTCPReuse(address string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, addr string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}
	return lc.Listen(context.Background(), "tcp", address)
}
