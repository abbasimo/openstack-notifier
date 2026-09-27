// Usage:
//
//	notifier              		run the service (environment variables over CONFIG_FILE, see docs/CONFIG.md)
//	notifier healthcheck   		GET /readyz on HTTP_ADDR; exit 0 when ready, 1 otherwise
//	notifier version       		print the build version

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	context.AfterFunc(ctx, stop)

}
