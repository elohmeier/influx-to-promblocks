package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/elohmeier/influx-to-promblocks/internal/influx"
)

type Config struct {
	InfluxURL                string
	InfluxUsername           string
	InfluxPassword           string
	Database                 string
	RetentionPolicy          string
	Measurements             []string
	Start                    string
	End                      string
	Window                   time.Duration
	BlockDuration            time.Duration
	ChunkSize                int
	Parallelism              int
	MaxFieldsPerQuery        int
	CompactOutput            bool
	IncludeBooleans          bool
	MetricPrefix             string
	MetricNameMode           string
	PreserveSourceLabels     bool
	DuplicateTimestampPolicy string
	OutputDir                string
	SeriesLabelArgs          []string
	LogLevel                 string
}

type Stats struct {
	Windows int
	Blocks  int
	Samples int64
	Series  int
}

type Importer struct {
	logger       *slog.Logger
	cfg          Config
	influx       *influx.Client
	seriesLabels map[string]string
}

type measurementSchema struct {
	Name   string
	Fields []influx.Field
}

func New(logger *slog.Logger, cfg Config) *Importer {
	return &Importer{logger: logger, cfg: cfg}
}

func (i *Importer) Run(ctx context.Context) (Stats, error) {
	if err := i.validate(); err != nil {
		return Stats{}, err
	}

	var err error
	i.seriesLabels, err = parseLabelArgs(i.cfg.SeriesLabelArgs)
	if err != nil {
		return Stats{}, fmt.Errorf("parse series labels: %w", err)
	}

	i.influx = &influx.Client{
		BaseURL:         i.cfg.InfluxURL,
		Username:        i.cfg.InfluxUsername,
		Password:        i.cfg.InfluxPassword,
		Database:        i.cfg.Database,
		RetentionPolicy: i.cfg.RetentionPolicy,
		HTTPClient:      newHTTPClient(i.cfg.Parallelism),
	}

	start, err := parseTimeArg(i.cfg.Start, time.Now())
	if err != nil {
		return Stats{}, fmt.Errorf("parse --start: %w", err)
	}
	end, err := parseTimeArg(i.cfg.End, time.Now())
	if err != nil {
		return Stats{}, fmt.Errorf("parse --end: %w", err)
	}
	if !start.Before(end) {
		return Stats{}, fmt.Errorf("--start must be before --end")
	}

	if err := os.MkdirAll(i.cfg.OutputDir, 0o755); err != nil {
		return Stats{}, err
	}

	schema, err := i.discoverSchema(ctx)
	if err != nil {
		return Stats{}, err
	}
	if len(schema) == 0 {
		i.logger.Warn("no numeric Influx fields found")
		return Stats{}, nil
	}
	i.logger.Info("schema discovered", "measurements", len(schema), "numeric_field_groups", countFields(schema))

	windows := buildWindows(start, end, i.cfg.Window)
	if i.cfg.CompactOutput {
		return i.copyWindowsCompacted(ctx, schema, start, end)
	}

	stats, _, err := i.copyWindows(ctx, i.cfg.OutputDir, schema, windows)
	return stats, err
}

func (i *Importer) validate() error {
	if strings.TrimSpace(i.cfg.Database) == "" {
		return fmt.Errorf("--database is required")
	}
	if strings.TrimSpace(i.cfg.Start) == "" {
		return fmt.Errorf("--start is required")
	}
	if strings.TrimSpace(i.cfg.End) == "" {
		return fmt.Errorf("--end is required")
	}
	if i.cfg.Window <= 0 {
		return fmt.Errorf("--window must be positive")
	}
	if i.cfg.BlockDuration <= 0 {
		return fmt.Errorf("--block-duration must be positive")
	}
	if i.cfg.Window > i.cfg.BlockDuration {
		return fmt.Errorf("--window must be less than or equal to --block-duration")
	}
	if i.cfg.ChunkSize <= 0 {
		return fmt.Errorf("--chunk-size must be positive")
	}
	if i.cfg.Parallelism == 0 {
		i.cfg.Parallelism = 1
	}
	if i.cfg.Parallelism < 0 {
		return fmt.Errorf("--parallelism must be positive")
	}
	if i.cfg.MaxFieldsPerQuery <= 0 {
		return fmt.Errorf("--max-fields-per-query must be positive")
	}
	i.cfg.MetricNameMode = normalizeMetricNameMode(i.cfg.MetricNameMode)
	switch i.cfg.MetricNameMode {
	case MetricNameModeMeasurementField, MetricNameModeField:
	default:
		return fmt.Errorf("--metric-name-mode must be one of %q, %q", MetricNameModeMeasurementField, MetricNameModeField)
	}
	switch i.cfg.DuplicateTimestampPolicy {
	case "error", "first":
	default:
		return fmt.Errorf("--duplicate-timestamp-policy must be one of error, first")
	}
	if strings.TrimSpace(i.cfg.OutputDir) == "" {
		return fmt.Errorf("--output-dir is required")
	}
	return nil
}

type windowRange struct {
	start time.Time
	end   time.Time
}

type windowResult struct {
	block copiedBlock
	stats Stats
	err   error
}

type copiedBlock struct {
	dir    string
	window windowRange
}

func newHTTPClient(parallelism int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if transport.MaxIdleConns < parallelism {
		transport.MaxIdleConns = parallelism
	}
	if transport.MaxIdleConnsPerHost < parallelism {
		transport.MaxIdleConnsPerHost = parallelism
	}
	return &http.Client{Transport: transport}
}

func buildWindows(start, end time.Time, window time.Duration) []windowRange {
	var windows []windowRange
	for windowStart := start; windowStart.Before(end); windowStart = windowStart.Add(window) {
		windowEnd := windowStart.Add(window)
		if windowEnd.After(end) {
			windowEnd = end
		}
		windows = append(windows, windowRange{start: windowStart, end: windowEnd})
	}
	return windows
}

func (i *Importer) copyWindows(ctx context.Context, outputDir string, schema []measurementSchema, windows []windowRange) (Stats, []copiedBlock, error) {
	if i.cfg.Parallelism == 1 {
		return i.copyWindowsSerial(ctx, outputDir, schema, windows)
	}
	return i.copyWindowsParallel(ctx, outputDir, schema, windows)
}

func (i *Importer) copyWindowsSerial(ctx context.Context, outputDir string, schema []measurementSchema, windows []windowRange) (Stats, []copiedBlock, error) {
	var stats Stats
	var blocks []copiedBlock
	for _, window := range windows {
		windowStats, blockDir, err := i.copyWindow(ctx, outputDir, schema, window.start, window.end)
		if err != nil {
			return stats, blocks, err
		}
		addWindowStats(&stats, windowStats)
		if blockDir != "" {
			blocks = append(blocks, copiedBlock{dir: blockDir, window: window})
		}
	}
	return stats, blocks, nil
}

func (i *Importer) copyWindowsParallel(ctx context.Context, outputDir string, schema []measurementSchema, windows []windowRange) (Stats, []copiedBlock, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := i.cfg.Parallelism
	if workers > len(windows) {
		workers = len(windows)
	}
	i.logger.Info("copying windows in parallel", "windows", len(windows), "parallelism", workers)

	jobs := make(chan windowRange)
	results := make(chan windowResult, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for window := range jobs {
				if ctx.Err() != nil {
					return
				}
				windowStats, blockDir, err := i.copyWindow(ctx, outputDir, schema, window.start, window.end)
				if err != nil {
					results <- windowResult{err: fmt.Errorf("copy window %s to %s: %w", window.start.Format(time.RFC3339), window.end.Format(time.RFC3339), err)}
					cancel()
					return
				}
				result := windowResult{stats: windowStats}
				if blockDir != "" {
					result.block = copiedBlock{dir: blockDir, window: window}
				}
				results <- result
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, window := range windows {
			select {
			case jobs <- window:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var stats Stats
	var blocks []copiedBlock
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		addWindowStats(&stats, result.stats)
		if result.block.dir != "" {
			blocks = append(blocks, result.block)
		}
	}
	return stats, blocks, firstErr
}

func addWindowStats(total *Stats, window Stats) {
	total.Windows++
	total.Blocks += window.Blocks
	total.Samples += window.Samples
	if window.Series > total.Series {
		total.Series = window.Series
	}
}

func (i *Importer) discoverSchema(ctx context.Context) ([]measurementSchema, error) {
	measurements := append([]string(nil), i.cfg.Measurements...)
	if len(measurements) == 0 {
		var err error
		measurements, err = i.influx.ShowMeasurements(ctx)
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(measurements)

	schema := make([]measurementSchema, 0, len(measurements))
	for _, measurement := range measurements {
		fields, err := i.influx.ShowFieldKeys(ctx, measurement)
		if err != nil {
			return nil, err
		}
		fields = filterNumericFields(fields, i.cfg.IncludeBooleans)
		if len(fields) == 0 {
			i.logger.Debug("skipping measurement without numeric fields", "measurement", measurement)
			continue
		}
		sort.Slice(fields, func(a, b int) bool { return fields[a].Key < fields[b].Key })
		schema = append(schema, measurementSchema{Name: measurement, Fields: fields})
	}
	return schema, nil
}

func (i *Importer) copyWindowsCompacted(ctx context.Context, schema []measurementSchema, start, end time.Time) (Stats, error) {
	tempDir, err := os.MkdirTemp(i.cfg.OutputDir, ".tmp-influx-to-promblocks-")
	if err != nil {
		return Stats{}, err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			i.logger.Warn("failed to remove temporary block directory", "dir", tempDir, "err", err)
		}
	}()

	finalWindows := buildWindows(start, end, i.cfg.BlockDuration)
	queryWindows := buildQueryWindows(finalWindows, i.cfg.Window)
	i.logger.Info(
		"copying query windows before compaction",
		"query_windows", len(queryWindows),
		"final_block_windows", len(finalWindows),
		"temporary_dir", tempDir,
	)

	stats, blocks, err := i.copyWindows(ctx, tempDir, schema, queryWindows)
	if err != nil {
		return stats, err
	}
	if len(blocks) == 0 {
		i.logger.Info("no temporary blocks to compact")
		return stats, nil
	}

	compactStats, err := i.compactCopiedBlocks(ctx, blocks, finalWindows)
	if err != nil {
		return stats, err
	}
	stats.Blocks = compactStats.Blocks
	stats.Samples = compactStats.Samples
	stats.Series = compactStats.Series
	return stats, nil
}

func buildQueryWindows(finalWindows []windowRange, queryWindow time.Duration) []windowRange {
	var queryWindows []windowRange
	for _, finalWindow := range finalWindows {
		queryWindows = append(queryWindows, buildWindows(finalWindow.start, finalWindow.end, queryWindow)...)
	}
	return queryWindows
}

type copiedBlockMeta struct {
	block copiedBlock
	meta  tsdb.BlockMeta
}

func (i *Importer) compactCopiedBlocks(ctx context.Context, blocks []copiedBlock, finalWindows []windowRange) (Stats, error) {
	blockMetas := make([]copiedBlockMeta, 0, len(blocks))
	for _, block := range blocks {
		meta, err := readBlockMeta(block.dir)
		if err != nil {
			return Stats{}, err
		}
		blockMetas = append(blockMetas, copiedBlockMeta{block: block, meta: meta})
	}

	compactor, err := tsdb.NewLeveledCompactor(ctx, nil, i.logger, []int64{i.cfg.BlockDuration.Milliseconds()}, chunkenc.NewPool(), nil)
	if err != nil {
		return Stats{}, err
	}

	var total Stats
	for _, finalWindow := range finalWindows {
		var group []copiedBlockMeta
		for _, block := range blockMetas {
			if !block.block.window.start.Before(finalWindow.start) && block.block.window.start.Before(finalWindow.end) {
				group = append(group, block)
			}
		}
		if len(group) == 0 {
			continue
		}

		stats, err := i.compactBlockGroup(ctx, compactor, finalWindow, group)
		if err != nil {
			return total, err
		}
		total.Blocks += stats.Blocks
		total.Samples += stats.Samples
		if stats.Series > total.Series {
			total.Series = stats.Series
		}
	}
	return total, nil
}

func (i *Importer) compactBlockGroup(ctx context.Context, compactor tsdb.Compactor, finalWindow windowRange, group []copiedBlockMeta) (Stats, error) {
	sort.Slice(group, func(a, b int) bool {
		return group[a].meta.MinTime < group[b].meta.MinTime
	})

	dirs := make([]string, 0, len(group))
	for _, block := range group {
		dirs = append(dirs, block.block.dir)
	}

	i.logger.Info(
		"compacting temporary blocks",
		"blocks", len(dirs),
		"start", finalWindow.start.Format(time.RFC3339),
		"end", finalWindow.end.Format(time.RFC3339),
	)

	if len(dirs) == 1 {
		finalDir := filepath.Join(i.cfg.OutputDir, filepath.Base(dirs[0]))
		if err := os.Rename(dirs[0], finalDir); err != nil {
			return Stats{}, err
		}
		meta := group[0].meta
		i.logger.Info("moved single temporary block", "block", meta.ULID.String(), "samples", meta.Stats.NumSamples, "series", meta.Stats.NumSeries, "dir", finalDir)
		return Stats{Blocks: 1, Samples: int64(meta.Stats.NumSamples), Series: int(meta.Stats.NumSeries)}, nil
	}

	ids, err := compactor.Compact(i.cfg.OutputDir, dirs, nil)
	if err != nil {
		return Stats{}, err
	}

	var stats Stats
	for _, id := range ids {
		bdir := filepath.Join(i.cfg.OutputDir, id.String())
		meta, err := readBlockMeta(bdir)
		if err != nil {
			return stats, err
		}
		stats.Blocks++
		stats.Samples += int64(meta.Stats.NumSamples)
		if int(meta.Stats.NumSeries) > stats.Series {
			stats.Series = int(meta.Stats.NumSeries)
		}
		i.logger.Info("wrote compacted block", "block", id.String(), "samples", meta.Stats.NumSamples, "series", meta.Stats.NumSeries, "dir", bdir)
	}
	return stats, nil
}

func readBlockMeta(dir string) (tsdb.BlockMeta, error) {
	f, err := os.Open(filepath.Join(dir, "meta.json"))
	if err != nil {
		return tsdb.BlockMeta{}, err
	}
	defer f.Close()

	var meta tsdb.BlockMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return tsdb.BlockMeta{}, err
	}
	return meta, nil
}

func (i *Importer) copyWindow(ctx context.Context, outputDir string, schema []measurementSchema, start, end time.Time) (Stats, string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return Stats{}, "", err
	}

	writer, err := tsdb.NewBlockWriter(i.logger, outputDir, i.cfg.BlockDuration.Milliseconds())
	if err != nil {
		return Stats{}, "", err
	}
	defer writer.Close()

	bw := &blockWindow{
		writer:          writer,
		app:             writer.Appender(ctx),
		refs:            map[string]storage.SeriesRef{},
		lastSamples:     map[string]lastSample{},
		staticLabels:    i.seriesLabels,
		metricPrefix:    i.cfg.MetricPrefix,
		metricNameMode:  i.cfg.MetricNameMode,
		preserveSource:  i.cfg.PreserveSourceLabels,
		duplicatePolicy: i.cfg.DuplicateTimestampPolicy,
	}

	i.logger.Info("copying window", "start", start.Format(time.RFC3339), "end", end.Format(time.RFC3339))
	for _, ms := range schema {
		for _, fields := range batches(ms.Fields, i.cfg.MaxFieldsPerQuery) {
			q := influx.SelectFieldsQuery(ms.Name, fields, start, end)
			fieldByColumn := make(map[string]influx.Field, len(fields))
			for _, field := range fields {
				fieldByColumn[field.Key] = field
			}
			if err := i.influx.Query(ctx, q, true, i.cfg.ChunkSize, func(resp influx.Response) error {
				return bw.appendResponse(ms.Name, fieldByColumn, resp, i.cfg.IncludeBooleans)
			}); err != nil {
				return Stats{}, "", err
			}
		}
	}

	if bw.samples == 0 {
		i.logger.Info("window had no samples", "start", start.Format(time.RFC3339), "end", end.Format(time.RFC3339))
		return Stats{}, "", nil
	}
	if err := bw.commit(); err != nil {
		return Stats{}, "", err
	}
	ulid, err := writer.Flush(ctx)
	if err != nil {
		if errors.Is(err, tsdb.ErrNoSeriesAppended) {
			return Stats{}, "", nil
		}
		return Stats{}, "", err
	}
	if ulid.String() == "00000000000000000000000000" {
		return Stats{}, "", nil
	}

	stats := Stats{Blocks: 1, Samples: bw.samples, Series: len(bw.refs)}
	bdir := filepath.Join(outputDir, ulid.String())
	i.logger.Info("wrote local block", "block", ulid.String(), "samples", bw.samples, "series", len(bw.refs), "dir", bdir)
	return stats, bdir, nil
}

type blockWindow struct {
	writer          *tsdb.BlockWriter
	app             storage.Appender
	refs            map[string]storage.SeriesRef
	lastSamples     map[string]lastSample
	staticLabels    map[string]string
	metricPrefix    string
	metricNameMode  string
	preserveSource  bool
	duplicatePolicy string
	samples         int64
	uncommitted     int
}

type lastSample struct {
	t int64
	v float64
}

func (b *blockWindow) appendResponse(measurement string, fieldByColumn map[string]influx.Field, resp influx.Response, includeBooleans bool) error {
	for _, result := range resp.Results {
		for _, series := range result.Series {
			timeIdx := -1
			fieldCols := map[int]influx.Field{}
			for idx, col := range series.Columns {
				if col == "time" {
					timeIdx = idx
					continue
				}
				if field, ok := fieldByColumn[col]; ok {
					fieldCols[idx] = field
				}
			}
			if timeIdx < 0 {
				continue
			}
			for _, row := range series.Values {
				if timeIdx >= len(row) {
					continue
				}
				ns, err := numberToInt64(row[timeIdx])
				if err != nil {
					return fmt.Errorf("parse influx timestamp: %w", err)
				}
				ms := ns / int64(time.Millisecond)
				for colIdx, field := range fieldCols {
					if colIdx >= len(row) || row[colIdx] == nil {
						continue
					}
					value, ok, err := influxValueToFloat(row[colIdx], field.Type, includeBooleans)
					if err != nil {
						return fmt.Errorf("parse field %s.%s: %w", measurement, field.Key, err)
					}
					if !ok {
						continue
					}
					name := metricName(b.metricNameMode, b.metricPrefix, measurement, field.Key)
					lset := buildLabels(name, measurement, field.Key, series.Tags, b.staticLabels, b.preserveSource)
					key := lset.String()
					if skip, err := b.handleDuplicate(key, ms, value); err != nil {
						return err
					} else if skip {
						continue
					}
					ref := b.refs[key]
					ref, err = b.app.Append(ref, lset, ms, value)
					if err != nil {
						return fmt.Errorf("append sample labels=%s timestamp_ms=%d: %w", lset.String(), ms, err)
					}
					b.refs[key] = ref
					b.samples++
					b.uncommitted++
				}
			}
		}
	}
	return nil
}

func (b *blockWindow) handleDuplicate(key string, t int64, v float64) (bool, error) {
	last, ok := b.lastSamples[key]
	if !ok || last.t != t {
		b.lastSamples[key] = lastSample{t: t, v: v}
		return false, nil
	}
	if last.v == v {
		return true, nil
	}
	switch b.duplicatePolicy {
	case "first":
		return true, nil
	default:
		return false, fmt.Errorf("series has multiple Influx samples that collapse to Prometheus timestamp %dms with different values", t)
	}
}

func (b *blockWindow) commit() error {
	if b.uncommitted == 0 {
		return nil
	}
	if err := b.app.Commit(); err != nil {
		return err
	}
	b.uncommitted = 0
	return nil
}

func filterNumericFields(fields []influx.Field, includeBooleans bool) []influx.Field {
	out := fields[:0]
	for _, field := range fields {
		switch strings.ToLower(field.Type) {
		case "float", "integer", "unsigned":
			out = append(out, field)
		case "boolean":
			if includeBooleans {
				out = append(out, field)
			}
		}
	}
	return out
}

func batches(fields []influx.Field, size int) [][]influx.Field {
	var out [][]influx.Field
	for len(fields) > 0 {
		n := size
		if len(fields) < n {
			n = len(fields)
		}
		out = append(out, fields[:n])
		fields = fields[n:]
	}
	return out
}

func influxValueToFloat(v any, fieldType string, includeBooleans bool) (float64, bool, error) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false, nil
		}
		return f, true, nil
	case float64:
		return x, true, nil
	case bool:
		if !includeBooleans || strings.ToLower(fieldType) != "boolean" {
			return 0, false, nil
		}
		if x {
			return 1, true, nil
		}
		return 0, true, nil
	case string:
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("unsupported JSON value type %T", v)
	}
}

func numberToInt64(v any) (int64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Int64()
	case float64:
		return int64(x), nil
	case int64:
		return x, nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported timestamp type %T", v)
	}
}

func parseTimeArg(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "now" {
		return now.UTC(), nil
	}
	if strings.HasPrefix(s, "-") {
		d, err := time.ParseDuration(strings.TrimPrefix(s, "-"))
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(-d).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("expected RFC3339/RFC3339Nano, YYYY-MM-DD, 'now', or negative duration")
}

func countFields(schema []measurementSchema) int {
	var n int
	for _, ms := range schema {
		n += len(ms.Fields)
	}
	return n
}
