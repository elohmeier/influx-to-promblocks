package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/elohmeier/influx-to-promblocks/internal/importer"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type repeatedStrings []string

func (r *repeatedStrings) String() string {
	return strings.Join(*r, ",")
}

func (r *repeatedStrings) Set(v string) error {
	*r = append(*r, v)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		printVersion()
		return nil
	}
	if len(os.Args) > 1 && (os.Args[1] == "export" || os.Args[1] == "copy") {
		return runExport(os.Args[2:])
	}
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help") {
		printUsage()
		return nil
	}
	return runExport(os.Args[1:])
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: influx-to-promblocks [export] [flags]\n\n")
	fmt.Fprintf(os.Stderr, "Exports numeric InfluxDB v1 fields into local Prometheus TSDB blocks.\n\n")
}

func printVersion() {
	fmt.Fprintf(os.Stdout, "influx-to-promblocks %s\ncommit: %s\nbuilt: %s\n", version, commit, date)
}

func runExport(args []string) error {
	var measurements repeatedStrings
	var seriesLabels repeatedStrings

	cfg := importer.Config{}

	fs := flag.NewFlagSet("export", flag.ExitOnError)
	fs.StringVar(&cfg.InfluxURL, "influx-url", "http://localhost:8086", "InfluxDB v1 base URL.")
	fs.StringVar(&cfg.InfluxUsername, "influx-username", "", "InfluxDB username.")
	fs.StringVar(&cfg.InfluxPassword, "influx-password", "", "InfluxDB password.")
	fs.StringVar(&cfg.Database, "database", "", "InfluxDB database to copy from.")
	fs.StringVar(&cfg.RetentionPolicy, "retention-policy", "", "InfluxDB retention policy. Empty uses the database default.")
	fs.Var(&measurements, "measurement", "Measurement to copy. May be repeated. Empty discovers all measurements.")
	fs.StringVar(&cfg.Start, "start", "", "Inclusive start time. RFC3339/RFC3339Nano or Unix duration offset like -24h.")
	fs.StringVar(&cfg.End, "end", "", "Exclusive end time. RFC3339/RFC3339Nano, Unix duration offset like -1h, or 'now'.")
	fs.DurationVar(&cfg.Window, "window", 2*time.Hour, "Influx query and output block window.")
	fs.DurationVar(&cfg.BlockDuration, "block-duration", 2*time.Hour, "Prometheus TSDB block duration.")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 10000, "Influx chunk_size for SELECT queries.")
	fs.IntVar(&cfg.Parallelism, "parallelism", 1, "Number of windows to copy concurrently.")
	fs.IntVar(&cfg.MaxFieldsPerQuery, "max-fields-per-query", 20, "Maximum number of fields selected per Influx query.")
	fs.BoolVar(&cfg.IncludeBooleans, "include-booleans", false, "Copy boolean Influx fields as 0/1 gauges.")
	fs.StringVar(&cfg.MetricPrefix, "metric-prefix", "", "Prefix to add to generated metric names.")
	fs.BoolVar(&cfg.PreserveSourceLabels, "preserve-source-labels", true, "Add influxdb_measurement and influxdb_field labels.")
	fs.StringVar(&cfg.DuplicateTimestampPolicy, "duplicate-timestamp-policy", "error", "Policy when nanosecond Influx samples collapse to the same Prometheus millisecond: error or first.")
	fs.StringVar(&cfg.OutputDir, "output-dir", "./blocks", "Directory that will contain generated Prometheus TSDB block directories.")
	fs.Var(&seriesLabels, "series-label", "Static Prometheus series label key=value or key=\"value\". May be repeated.")
	fs.StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error.")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg.Measurements = measurements
	cfg.SeriesLabelArgs = seriesLabels

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return fmt.Errorf("unsupported --log-level %q", cfg.LogLevel)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, cancel := signalContext()
	defer cancel()

	stats, err := importer.New(logger, cfg).Run(ctx)
	if err != nil {
		return err
	}
	logger.Info("export completed",
		"windows", stats.Windows,
		"blocks", stats.Blocks,
		"samples", stats.Samples,
		"series", stats.Series,
		"output_dir", cfg.OutputDir,
	)
	return nil
}
