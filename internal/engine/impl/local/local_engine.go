package local

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine"
	"github.com/agnosticeng/agnostic-blockchain-etl/internal/utils"
	"github.com/mholt/archiver/v4"
	slogctx "github.com/veqryn/slog-context"
	"gopkg.in/yaml.v3"
)

type LocalEngineConfig struct {
	ConnPoolConfig
	BinaryPath     string
	WorkingDir     string
	Env            map[string]string
	Bundles        []string
	BundlesPath    string
	DisableCleanup bool
	ServerSettings map[string]any
}

type LocalEngine struct {
	conf   LocalEngineConfig
	logger *slog.Logger
	cmd    *exec.Cmd
	pool   *ConnPool
}

func NewLocalEngine(ctx context.Context, conf LocalEngineConfig) (*LocalEngine, error) {
	var logger = slogctx.FromCtx(ctx)

	if len(conf.BinaryPath) == 0 {
		conf.BinaryPath = "clickhouse"
	}

	if len(conf.BundlesPath) == 0 {
		path, err := os.UserCacheDir()

		if err != nil {
			return nil, err
		}

		conf.BundlesPath = filepath.Join(path, "agnostic-blockchain-etl/bundles")
	}

	if !filepath.IsAbs(conf.BinaryPath) {
		path, err := exec.LookPath(conf.BinaryPath)

		if err != nil {
			return nil, err
		}

		conf.BinaryPath = path
	}

	if len(conf.WorkingDir) == 0 {
		p, err := os.MkdirTemp(os.TempDir(), "*")

		if err != nil {
			return nil, err
		}

		conf.WorkingDir = p
		logger.Debug("created temporary working dir", "path", conf.WorkingDir)
	} else {
		if err := os.MkdirAll(conf.WorkingDir, 0700); err != nil {
			return nil, err
		}
	}

	if len(conf.Dsn) == 0 {
		conf.Dsn = "tcp://127.0.0.1:9001/default"
	}

	var (
		defaultSettings = map[string]interface{}{
			"path": "./",
			"user_defined_executable_functions_config": "*_function.*ml",
			"listen_host": "127.0.0.1",
			"tcp_port":    9001,
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
	maps.Copy(finalSettings, ch.NormalizeSettings(conf.ServerSettings))

	data, err := yaml.Marshal(finalSettings)

	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(conf.WorkingDir, "config.yaml"), data, 0644); err != nil {
		return nil, err
	}

	if len(conf.Bundles) > 0 {
		if err := os.MkdirAll(conf.BundlesPath, 0700); err != nil {
			return nil, err
		}

		for _, remote := range conf.Bundles {
			var local = filepath.Join(conf.BundlesPath, utils.SHA256Sum(remote))

			logger.Debug("downloading bundle", "url", remote, "path", local)

			if err := utils.CachedDownload(ctx, remote, local); err != nil {
				return nil, fmt.Errorf("error while downloading bundle %s: %w", remote, err)
			}

			f, err := os.Open(local)

			if err != nil {
				return nil, err
			}

			format, r, err := archiver.Identify(ctx, local, f)

			if err != nil {
				return nil, err
			}

			if ex, ok := format.(archiver.Extractor); ok {
				logger.Debug("extracting bundle", "path", local)

				if err := ex.Extract(ctx, r, extractBundle(conf.WorkingDir)); err != nil {
					return nil, err
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
	cmd.Env = slices.Clone(os.Environ())

	for k, v := range conf.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
	}

	return &LocalEngine{
		conf:   conf,
		logger: logger,
		cmd:    cmd,
		pool:   NewConnPool(conf.ConnPoolConfig),
	}, nil
}

func (eng *LocalEngine) Start() error {
	return eng.cmd.Start()
}

func (eng *LocalEngine) Stop() {
	eng.cmd.Process.Signal(syscall.SIGTERM)
}

func (eng *LocalEngine) Wait() error {
	if !eng.conf.DisableCleanup {
		defer os.RemoveAll(eng.conf.WorkingDir)
	}

	eng.pool.Close()
	var err = eng.cmd.Wait()

	if err == nil {
		return nil
	}

	content, _ := os.ReadFile(filepath.Join(eng.conf.WorkingDir, "clickhouse-server-error.log"))
	eng.logger.Error("clickhouse server error", "log", string(content))
	return err
}

func (eng *LocalEngine) AcquireConn() (engine.Conn, error) {
	return eng.pool.Acquire()
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
