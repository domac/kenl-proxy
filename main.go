package main

import (
	log "github.com/domac/kenl-proxy/logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	log.GetLogger().Info("shutdown kenl-proxy")
}
