package message

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendGridSender_UsesConfiguredTimeout(t *testing.T) {
	const (
		serverDelay   = 2 * time.Second
		clientTimeout = 50 * time.Millisecond
		maxElapsed    = serverDelay / 2
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(serverDelay)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sender, err := newSendGridSender(SendGridConfig{
		ApiKey:  "key",
		From:    "noreply@example.com",
		Timeout: clientTimeout,
	})
	if err != nil {
		t.Fatalf("new sendgrid sender: %v", err)
	}
	sender.host = srv.URL

	start := time.Now()
	err = sender.Send(context.Background(), EmailMessage{
		To:      "user@example.com",
		Subject: "hello",
		Text:    "body",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > maxElapsed {
		t.Fatalf("expected configured timeout to cut request short, took %v", elapsed)
	}
}
