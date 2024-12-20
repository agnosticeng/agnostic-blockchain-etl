package ch

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/mholt/archiver/v4"
	slogctx "github.com/veqryn/slog-context"
	"gopkg.in/yaml.v3"
)

type LocalExecutorConfig struct {
	BinaryPath     string
	WorkingDir     string
	Bundles        []string
	BundlesPath    string
	DisableCleanup bool
	Settings       clickhouse.Settings
}

func RunLocalExecutor(ctx context.Context, conf LocalExecutorConfig) error {
	var logger = slogctx.FromCtx(ctx)

	if len(conf.BinaryPath) == 0 {
		conf.BinaryPath = "clickhouse"
	}

	if len(conf.BundlesPath) == 0 {
		path, err := os.UserCacheDir()

		if err != nil {
			return err
		}

		conf.BundlesPath = filepath.Join(path, "agnostic-blockchain-etl/bundles")
	}

	if !filepath.IsAbs(conf.BinaryPath) {
		path, err := exec.LookPath(conf.BinaryPath)

		if err != nil {
			return err
		}

		conf.BinaryPath = path
	}

	if len(conf.WorkingDir) == 0 {
		p, err := os.MkdirTemp(os.TempDir(), "*")

		if err != nil {
			return err
		}

		conf.WorkingDir = p
		logger.Debug("created temporary working dir", "path", conf.WorkingDir)
	} else {
		if err := os.MkdirAll(conf.WorkingDir, 0700); err != nil {
			return err
		}
	}

	if !conf.DisableCleanup {
		defer os.RemoveAll(conf.WorkingDir)
	}

	var (
		defaultSettings = map[string]interface{}{
			"path": "./",
			"user_defined_executable_functions_config": "*_function.*ml",
			"listen_host": "127.0.0.1",
			"tcp_port":    9000,
			"profiles": map[string]interface{}{
				"default": map[string]interface{}{},
			},
			"users": map[string]interface{}{
				"default": map[string]interface{}{
					"password": "",
				},
			},
			"s3": map[string]interface{}{
				"ovh-rbx": map[string]interface{}{
					"endpoint": "https://s3.rbx.io.cloud.ovh.net",
					"region":   "rbx",
				},
			},
		}
		finalSettings = make(map[string]interface{})
	)

	maps.Copy(finalSettings, defaultSettings)
	maps.Copy(finalSettings, NormalizeSettings(conf.Settings))

	data, err := yaml.Marshal(finalSettings)

	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(conf.WorkingDir, "config.yaml"), data, 0644); err != nil {
		return err
	}

	if len(conf.Bundles) > 0 {
		if err := os.MkdirAll(conf.BundlesPath, 0700); err != nil {
			return err
		}

		for _, remote := range conf.Bundles {
			var local = filepath.Join(conf.BundlesPath, utils.SHA256Sum(remote))

			logger.Debug("downloading bundle", "url", remote, "path", local)

			if err := utils.CachedDownload(remote, local); err != nil {
				return fmt.Errorf("error while downloading bundle %s: %w", remote, err)
			}

			f, err := os.Open(local)

			if err != nil {
				return err
			}

			format, r, err := archiver.Identify(ctx, local, f)

			if err != nil {
				return err
			}

			if ex, ok := format.(archiver.Extractor); ok {
				logger.Debug("extracting bundle", "path", local)

				if err := ex.Extract(ctx, r, extractBundle(conf.WorkingDir)); err != nil {
					return err
				}
			}
		}
	}

	var cmd = exec.Command(
		conf.BinaryPath,
		"server",
		"--config-file=config.yaml",
		"--log-file=clickhouse-server.log",
		"--errorlog-file=clickhouse-server-error.log",
	)

	cmd.Dir = conf.WorkingDir

	go func() {
		<-ctx.Done()
		cmd.Process.Signal(syscall.SIGTERM)
	}()

	logger.Debug("running clickhouse server")

	err = cmd.Run()

	if err == nil {
		return nil
	}

	content, _ := os.ReadFile(filepath.Join(conf.WorkingDir, "clickhouse-server-error.log"))
	logger.Error("clickhouse server error", "log", string(content))
	return err
}

func extractBundle(basePath string) func(ctx context.Context, info archiver.FileInfo) error {
	return func(ctx context.Context, info archiver.FileInfo) error {
		var dstPath string

		switch filepath.Dir(info.NameInArchive) {
		case "/etc/clickhouse-server", "etc/clickhouse-server":
			dstPath = filepath.Join(basePath, filepath.Base(info.NameInArchive))
		case "/var/lib/clickhouse/user_defined", "var/lib/clickhouse/user_defined":
			dstPath = filepath.Join(basePath, "user_defined", filepath.Base(info.NameInArchive))
		case "/var/lib/clickhouse/user_scripts", "var/lib/clickhouse/user_scripts":
			dstPath = filepath.Join(basePath, "user_scripts", filepath.Base(info.NameInArchive))
		default:
			return nil
		}

		if err := os.MkdirAll(filepath.Dir(dstPath), 0744); err != nil {
			return err
		}

		r, err := info.Open()

		if err != nil {
			return err
		}

		defer r.Close()

		w, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, info.FileInfo.Mode())

		if err != nil {
			return err
		}

		if _, err := io.Copy(w, r); err != nil {
			return err
		}

		return w.Close()
	}
}
