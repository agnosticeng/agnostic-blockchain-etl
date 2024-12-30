package utils

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
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

func CachedDownload(ctx context.Context, src string, dest string) error {
	srcUrl, err := url.Parse(src)

	if err != nil {
		return err
	}

	if _, err := os.Stat(dest); err == nil {
		return nil
	}

	return objstr.FromContextOrDefault(ctx).Copy(ctx, srcUrl, &url.URL{Scheme: "file", Path: dest})
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
