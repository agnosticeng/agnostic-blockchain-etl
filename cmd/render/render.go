package render

import (
	"fmt"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/urfave/cli/v2"
)

var Flags = []cli.Flag{
	&cli.StringSliceFlag{Name: "var"},
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "render",
		Flags: Flags,
		Action: func(ctx *cli.Context) error {
			var (
				path = ctx.Args().Get(0)
				vars = utils.ParseKeyValues(ctx.StringSlice("var"), "=")
			)

			if len(path) == 0 {
				return fmt.Errorf("a path must be specified")
			}

			tmpl, err := utils.BuildTemplate(path)

			if err != nil {
				return err
			}

			for _, tmpl := range tmpl.Templates() {
				fmt.Println("--------------------------------------------------------------------------------")
				fmt.Println(tmpl.Name())
				fmt.Println("--------------------------------------------------------------------------------")

				str, err := utils.RenderTemplate(tmpl, tmpl.Name(), vars)

				if err != nil {
					return err
				}

				fmt.Println(str)
			}

			return nil
		},
	}
}
