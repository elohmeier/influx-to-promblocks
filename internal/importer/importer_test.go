package importer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/elohmeier/influx-to-promblocks/internal/influx"
)

func TestImporterCopiesWindowsInParallel(t *testing.T) {
	var inFlight int64
	var maxInFlight int64
	timeLowerBound := regexp.MustCompile(`time >= ([0-9]+)`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		q := r.Form.Get("q")
		switch {
		case strings.HasPrefix(q, "SHOW FIELD KEYS"):
			_, _ = w.Write([]byte(`{"results":[{"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`))
		case strings.HasPrefix(q, "SELECT"):
			current := atomic.AddInt64(&inFlight, 1)
			for {
				previous := atomic.LoadInt64(&maxInFlight)
				if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&inFlight, -1)

			matches := timeLowerBound.FindStringSubmatch(q)
			if len(matches) != 2 {
				t.Fatalf("query did not contain lower time bound: %s", q)
			}
			_, _ = fmt.Fprintf(w, `{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[[%s,1]]}]}]}`, matches[1])
		default:
			t.Fatalf("unexpected query: %s", q)
		}
	}))
	defer srv.Close()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{
		InfluxURL:                srv.URL,
		Database:                 "db",
		Measurements:             []string{"cpu"},
		Start:                    start.Format(time.RFC3339),
		End:                      start.Add(4 * time.Hour).Format(time.RFC3339),
		Window:                   time.Hour,
		BlockDuration:            time.Hour,
		ChunkSize:                10,
		Parallelism:              2,
		MaxFieldsPerQuery:        20,
		PreserveSourceLabels:     true,
		DuplicateTimestampPolicy: "error",
		OutputDir:                t.TempDir(),
	}
	stats, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Windows != 4 || stats.Blocks != 4 || stats.Samples != 4 || stats.Series != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if got := atomic.LoadInt64(&maxInFlight); got < 2 {
		t.Fatalf("max concurrent SELECT requests = %d, want at least 2", got)
	}
}

func TestValidateRejectsUnsupportedMetricNameMode(t *testing.T) {
	cfg := Config{
		Database:                 "db",
		Start:                    "2025-01-01T00:00:00Z",
		End:                      "2025-01-01T01:00:00Z",
		Window:                   time.Hour,
		BlockDuration:            time.Hour,
		ChunkSize:                10,
		Parallelism:              1,
		MaxFieldsPerQuery:        20,
		MetricNameMode:           "unknown",
		DuplicateTimestampPolicy: "error",
		OutputDir:                t.TempDir(),
	}

	err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg).validate()
	if err == nil || !strings.Contains(err.Error(), "--metric-name-mode") {
		t.Fatalf("validate() error = %v, want metric-name-mode error", err)
	}
}

func TestBlockWindowAppendResponseUsesFieldMetricNameMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	writer, err := tsdb.NewBlockWriter(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		dir,
		time.Hour.Milliseconds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	bw := &blockWindow{
		writer:          writer,
		app:             writer.Appender(ctx),
		refs:            map[string]storage.SeriesRef{},
		lastSamples:     map[string]lastSample{},
		metricNameMode:  MetricNameModeField,
		preserveSource:  true,
		duplicatePolicy: "error",
	}

	resp := influx.Response{
		Results: []influx.Result{{
			Series: []influx.Series{{
				Name:    "remote_write_archive",
				Columns: []string{"time", "service_requests_total"},
				Values: [][]any{{
					time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					float64(1),
				}},
			}},
		}},
	}

	err = bw.appendResponse("remote_write_archive", map[string]influx.Field{
		"service_requests_total": {
			Key:  "service_requests_total",
			Type: "float",
		},
	}, resp, false)
	if err != nil {
		t.Fatal(err)
	}
	for series := range bw.refs {
		if !strings.Contains(series, `__name__="service_requests_total"`) {
			t.Fatalf("series labels = %s, want field metric name", series)
		}
		if strings.Contains(series, "remote_write_archive_service_requests_total") {
			t.Fatalf("series labels = %s, unexpectedly used measurement-field metric name", series)
		}
		return
	}
	t.Fatal("no series appended")
}

func TestBlockWindowAppendResponseDoesNotRejectEarlierSeriesAfterLargeBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	writer, err := tsdb.NewBlockWriter(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		dir,
		(24 * time.Hour).Milliseconds(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	bw := &blockWindow{
		writer:          writer,
		app:             writer.Appender(ctx),
		refs:            map[string]storage.SeriesRef{},
		lastSamples:     map[string]lastSample{},
		preserveSource:  true,
		duplicatePolicy: "error",
	}

	start := time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC)
	resp := influx.Response{
		Results: []influx.Result{{
			Series: make([]influx.Series, 36),
		}},
	}
	for seriesIdx := range resp.Results[0].Series {
		series := influx.Series{
			Name:    "remote_write_archive",
			Tags:    map[string]string{"pod": fmt.Sprintf("pod-%02d", seriesIdx)},
			Columns: []string{"time", "request_duration_seconds_bucket"},
		}
		for sampleIdx := 0; sampleIdx < 24*12; sampleIdx++ {
			series.Values = append(series.Values, []any{
				start.Add(time.Duration(sampleIdx) * 5 * time.Minute).UnixNano(),
				float64(seriesIdx*1000 + sampleIdx),
			})
		}
		resp.Results[0].Series[seriesIdx] = series
	}

	err = bw.appendResponse("remote_write_archive", map[string]influx.Field{
		"request_duration_seconds_bucket": {Key: "request_duration_seconds_bucket", Type: "float"},
	}, resp, false)
	if err != nil {
		t.Fatalf("appendResponse() returned error after internal batch commit: %v", err)
	}
	if err := bw.commit(); err != nil {
		t.Fatalf("commit() returned error: %v", err)
	}
	if _, err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush() returned error: %v", err)
	}
}
