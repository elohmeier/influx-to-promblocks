package influx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueryDecodesChunkedObjects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("path = %s, want /query", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := r.Form.Get("chunked"); got != "true" {
			t.Fatalf("chunked = %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[[1000,1]]}]}]}` + "\n"))
		_, _ = w.Write([]byte(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[[2000,2]]}]}]}` + "\n"))
	}))
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, Database: "db"}
	var chunks int
	var values int
	err := client.Query(context.Background(), "SELECT value FROM cpu", true, 10, func(resp Response) error {
		chunks++
		for _, result := range resp.Results {
			for _, series := range result.Series {
				values += len(series.Values)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if chunks != 2 || values != 2 {
		t.Fatalf("chunks=%d values=%d, want chunks=2 values=2", chunks, values)
	}
}

func TestShowFieldKeys(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := r.Form.Get("q"); got != `SHOW FIELD KEYS FROM "cpu"` {
			t.Fatalf("query = %q", got)
		}
		_, _ = w.Write([]byte(`{"results":[{"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["usage","float"],["ok","boolean"]]}]}]}`))
	}))
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, Database: "db"}
	fields, err := client.ShowFieldKeys(context.Background(), "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 || fields[0].Key != "usage" || fields[0].Type != "float" || fields[1].Key != "ok" {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestSelectFieldsQuery(t *testing.T) {
	q := SelectFieldsQuery("cpu load", []Field{{Key: "usage idle"}, {Key: "system"}}, time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC())
	want := `SELECT "usage idle","system" FROM "cpu load" WHERE time >= 1000000000 AND time < 2000000000 GROUP BY *`
	if q != want {
		t.Fatalf("query = %q, want %q", q, want)
	}
}
