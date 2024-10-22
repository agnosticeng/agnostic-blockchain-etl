package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/agnosticeng/agnostic-blockchain-etl/cmd/render"
	"github.com/agnosticeng/agnostic-blockchain-etl/cmd/run"
	"github.com/agnosticeng/panicsafe"
	"github.com/agnosticeng/slogcli"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:   "agnostic-blockchain-etl",
		Flags:  slogcli.SlogFlags(),
		Before: slogcli.SlogBefore,
		Commands: []*cli.Command{
			run.Command(),
			render.Command(),
		},
	}

	var err = panicsafe.Recover(func() error { return app.Run(os.Args) })

	if err != nil {
		slog.Error(fmt.Sprintf("%v", err))
		os.Exit(1)
	}
}
