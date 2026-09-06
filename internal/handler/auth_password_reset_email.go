package handler

import (
	"context"
	"crypto/tls"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"time"
)

func (h *AuthHandler) sendConfiguredPasswordResetEmail(ctx context.Context, recipient, code string) error {
	cfg := h.configInfo.Auth.PasswordReset
	to, err := mail.ParseAddress(recipient)
	if err != nil {
		return err
	}
	from, err := mail.ParseAddress(cfg.From)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(cfg.SMTPHost, fmt.Sprintf("%d", cfg.SMTPPort))
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	if cfg.SMTPPort == 465 {
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: &tls.Config{ServerName: cfg.SMTPHost, MinVersion: tls.VersionTLS12}}
		conn, err = tlsDialer.DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	deadline := time.Now().Add(20 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	client, err := smtp.NewClient(conn, cfg.SMTPHost)
	if err != nil {
		return err
	}
	defer client.Close()
	if cfg.SMTPPort != 465 {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return fmt.Errorf("SMTP server requires STARTTLS support")
		}
		if err := client.StartTLS(&tls.Config{ServerName: cfg.SMTPHost, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if err := client.Auth(smtp.PlainAuth("", cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPHost)); err != nil {
		return err
	}
	return deliverPasswordResetMessage(client, from, to, code)
}

func deliverPasswordResetMessage(client *smtp.Client, from, to *mail.Address, code string) error {
	if err := client.Mail(from.Address); err != nil {
		return err
	}
	if err := client.Rcpt(to.Address); err != nil {
		return err
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	subject := mime.QEncoding.Encode("UTF-8", "WeKnora 密码重置验证码")
	body := fmt.Sprintf("您的 WeKnora 密码重置验证码是：%s\r\n\r\n验证码 10 分钟内有效。如非本人操作，请忽略此邮件。", code)
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n",
		from.String(), to.String(), subject, body)
	if _, err := w.Write([]byte(message)); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	// DATA's successful final response is the delivery acceptance boundary.
	// A later connection/QUIT failure must not invalidate an already sent code.
	_ = client.Quit()
	return nil
}
