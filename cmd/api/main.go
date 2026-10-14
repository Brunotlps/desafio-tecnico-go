// Comando api: servidor HTTP do serviço de apostas.
package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var app *fx.App = fx.New(bootstrap.Options(cfg))
	app.Run() // trata SIGINT/SIGTERM e encerra respeitando fx.StopTimeout
}
