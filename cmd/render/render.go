package render

import (
	"fmt"
	"os"
	"text/template"

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

			fmt.Println(ctx.StringSlice("var"))
			fmt.Println(vars)

			if len(path) == 0 {
				return fmt.Errorf("a path must be specified")
			}

			stat, err := os.Stat(path)

			if err != nil {
				return err
			}

			if !stat.IsDir() {
				return fmt.Errorf("path must point to a directory of SQL template files")
			}

			tmpl, err := template.ParseFS(os.DirFS(path), "*.sql")

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
