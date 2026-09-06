package handler

import (
	"io"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPasswordResetSMTPAcceptanceSurvivesQuitFailure(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(3 * time.Second))
	done := make(chan string, 1)
	go func() {
		defer serverConn.Close()
		server := textproto.NewConn(serverConn)
		_ = server.PrintfLine("220 test SMTP")
		var message string
		for {
			line, err := server.ReadLine()
			if err != nil {
				done <- message
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"):
				_ = server.PrintfLine("250 OK")
			case line == "DATA":
				_ = server.PrintfLine("354 send data")
				body, _ := io.ReadAll(server.DotReader())
				message = string(body)
				_ = server.PrintfLine("250 message accepted")
			case line == "QUIT":
				_ = server.PrintfLine("500 simulated quit failure")
				done <- message
				return
			}
		}
	}()
	client, err := smtp.NewClient(clientConn, "localhost")
	require.NoError(t, err)
	err = deliverPasswordResetMessage(client, &mail.Address{Address: "sender@example.com"}, &mail.Address{Address: "recipient@example.com"}, "123456")
	require.NoError(t, err)
	require.Contains(t, <-done, "123456")
}
