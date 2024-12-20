package utils

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/utils"
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

func CachedDownload(src string, dest string) error {
	if _, err := os.Stat(dest); err == nil {
		return nil
	}

	return Download(src, dest)
}

func Download(src string, dest string) error {
	u, err := url.Parse(src)

	if err != nil {
		return err
	}

	var r io.ReadCloser

	switch u.Scheme {
	case "", "file":
		r, err = os.Open(u.Path)

		if err != nil {
			return err
		}
	case "http", "https":
		resp, err := http.Get(u.String())

		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			return fmt.Errorf("bad status code: %d", resp.StatusCode)
		}

		r = resp.Body
	default:
		return fmt.Errorf("unhandled url scheme: %s", u.Scheme)
	}

	defer r.Close()

	w, err := os.Create(dest)

	if err != nil {
		return err
	}

	if _, err := io.Copy(w, r); err != nil {
		return err
	}

	return w.Close()
}

func SHA256Sum(s string) string {
	var h = sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func LoadTemplates(ctx context.Context, target *url.URL) (*template.Template, error) {
	var (
		os   = objstr.FromContextOrDefault(ctx)
		tmpl = template.New("pipeline").Option("missingkey=default").Funcs(sprig.FuncMap())
	)

	files, err := os.ListPrefix(ctx, target)

	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if filepath.Ext(file.URL.Path) != ".sql" {
			continue
		}

		content, err := utils.ReadObject(ctx, os, file.URL)

		if err != nil {
			return nil, err
		}

		if _, err := tmpl.New(filepath.Base(file.URL.Path)).Parse(string(content)); err != nil {
			return nil, err
		}
	}

	return tmpl, nil
}
