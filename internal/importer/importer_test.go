package importer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/elohmeier/influx-to-promblocks/internal/influx"
)

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
			Name:    "prometheus_monitoring_ai",
			Tags:    map[string]string{"pod": fmt.Sprintf("pod-%02d", seriesIdx)},
			Columns: []string{"time", "kid_in_gefleistung_eur_bucket"},
		}
		for sampleIdx := 0; sampleIdx < 24*12; sampleIdx++ {
			series.Values = append(series.Values, []any{
				start.Add(time.Duration(sampleIdx) * 5 * time.Minute).UnixNano(),
				float64(seriesIdx*1000 + sampleIdx),
			})
		}
		resp.Results[0].Series[seriesIdx] = series
	}

	err = bw.appendResponse("prometheus_monitoring_ai", map[string]influx.Field{
		"kid_in_gefleistung_eur_bucket": {Key: "kid_in_gefleistung_eur_bucket", Type: "float"},
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
