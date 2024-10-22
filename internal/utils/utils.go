package utils

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"text/template"

	"github.com/agnosticeng/agnostic-blockchain-etl/examples"
)

func ParseKeyValues(kvs []string, separator string) map[string]interface{} {
	var m = make(map[string]interface{})

	for _, kv := range kvs {
		var k, v, _ = strings.Cut(kv, separator)
		m[k] = v
	}

	return m
}

func RenderTemplate(tmpl *template.Template, name string, vars map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	if err := tmpl.ExecuteTemplate(&buf, name, vars); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func BuildTemplate(path string) (*template.Template, error) {
	var _fs fs.FS

	if strings.HasPrefix(path, "examples") {
		_sub, err := fs.Sub(examples.FS, strings.TrimPrefix(path, "examples://"))

		if err != nil {
			return nil, err
		}

		_fs = _sub
	} else {
		stat, err := os.Stat(path)

		if err != nil {
			return nil, err
		}

		if !stat.IsDir() {
			return nil, fmt.Errorf("path must point to a directory of SQL template files")
		}

		_fs = os.DirFS(path)
	}

	tmpl, err := template.ParseFS(_fs, "*.sql")

	if err != nil {
		return nil, err
	}

	return tmpl, nil
}
