package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	BaseURL         string
	Username        string
	Password        string
	Database        string
	RetentionPolicy string
	HTTPClient      *http.Client
}

type Field struct {
	Key  string
	Type string
}

type Response struct {
	Results []Result `json:"results"`
}

type Result struct {
	StatementID int      `json:"statement_id,omitempty"`
	Series      []Series `json:"series,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]any           `json:"values,omitempty"`
}

func (c *Client) Query(ctx context.Context, q string, chunked bool, chunkSize int, handle func(Response) error) error {
	if strings.TrimSpace(c.Database) == "" {
		return fmt.Errorf("database is required")
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil {
		return fmt.Errorf("parse influx URL: %w", err)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/query"

	form := url.Values{}
	form.Set("db", c.Database)
	if c.RetentionPolicy != "" {
		form.Set("rp", c.RetentionPolicy)
	}
	form.Set("q", q)
	form.Set("epoch", "ns")
	if chunked {
		form.Set("chunked", "true")
		if chunkSize > 0 {
			form.Set("chunk_size", fmt.Sprintf("%d", chunkSize))
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.Username != "" || c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 0}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return fmt.Errorf("influx query failed with HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	for {
		var out Response
		err := dec.Decode(&out)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode influx response: %w", err)
		}
		for _, result := range out.Results {
			if result.Error != "" {
				return fmt.Errorf("influx query error for %q: %s", q, result.Error)
			}
		}
		if handle != nil {
			if err := handle(out); err != nil {
				return err
			}
		}
	}
}

func (c *Client) ShowMeasurements(ctx context.Context) ([]string, error) {
	var measurements []string
	err := c.Query(ctx, "SHOW MEASUREMENTS", false, 0, func(resp Response) error {
		for _, result := range resp.Results {
			for _, series := range result.Series {
				nameIdx := columnIndex(series.Columns, "name")
				if nameIdx < 0 {
					continue
				}
				for _, row := range series.Values {
					if nameIdx >= len(row) {
						continue
					}
					if s, ok := row[nameIdx].(string); ok {
						measurements = append(measurements, s)
					}
				}
			}
		}
		return nil
	})
	return measurements, err
}

func (c *Client) ShowFieldKeys(ctx context.Context, measurement string) ([]Field, error) {
	q := "SHOW FIELD KEYS FROM " + QuoteIdent(measurement)
	var fields []Field
	err := c.Query(ctx, q, false, 0, func(resp Response) error {
		for _, result := range resp.Results {
			for _, series := range result.Series {
				keyIdx := columnIndex(series.Columns, "fieldKey")
				typeIdx := columnIndex(series.Columns, "fieldType")
				if keyIdx < 0 || typeIdx < 0 {
					continue
				}
				for _, row := range series.Values {
					if keyIdx >= len(row) || typeIdx >= len(row) {
						continue
					}
					key, keyOK := row[keyIdx].(string)
					typ, typeOK := row[typeIdx].(string)
					if keyOK && typeOK {
						fields = append(fields, Field{Key: key, Type: typ})
					}
				}
			}
		}
		return nil
	})
	return fields, err
}

func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

func SelectFieldsQuery(measurement string, fields []Field, start, end time.Time) string {
	selects := make([]string, 0, len(fields))
	for _, field := range fields {
		selects = append(selects, QuoteIdent(field.Key))
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE time >= %d AND time < %d GROUP BY *",
		strings.Join(selects, ","),
		QuoteIdent(measurement),
		start.UnixNano(),
		end.UnixNano(),
	)
}

func columnIndex(columns []string, name string) int {
	for i, col := range columns {
		if col == name {
			return i
		}
	}
	return -1
}
