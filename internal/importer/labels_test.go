package importer

import "testing"

func TestMetricName(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		prefix      string
		measurement string
		field       string
		want        string
	}{
		{name: "value field uses measurement", measurement: "cpu", field: "value", want: "cpu"},
		{name: "non value field appends field", measurement: "cpu", field: "usage idle", want: "cpu_usage_idle"},
		{name: "invalid leading digit", measurement: "1cpu", field: "value", want: "influx_1cpu"},
		{name: "prefix", prefix: "legacy_", measurement: "cpu", field: "load-1", want: "legacy_cpu_load_1"},
		{
			name:        "field mode uses prometheus-style influx field directly",
			mode:        MetricNameModeField,
			measurement: "remote_write_archive",
			field:       "service_requests_total",
			want:        "service_requests_total",
		},
		{
			name:        "field mode uses value field directly",
			mode:        MetricNameModeField,
			measurement: "cpu",
			field:       "value",
			want:        "value",
		},
		{
			name:        "field mode still applies prefix and sanitization",
			mode:        MetricNameModeField,
			prefix:      "legacy_",
			measurement: "remote_write_archive",
			field:       "9 bad field",
			want:        "legacy_9_bad_field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode := tt.mode
			if mode == "" {
				mode = MetricNameModeMeasurementField
			}
			if got := metricName(mode, tt.prefix, tt.measurement, tt.field); got != tt.want {
				t.Fatalf("metricName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildLabelsSanitizesAndPreservesSource(t *testing.T) {
	got := buildLabels("cpu_usage", "cpu/load", "usage idle", map[string]string{
		"host.name": "a",
		"host-name": "b",
		"2zone":     "z",
	}, map[string]string{"env": "prod"}, true).Map()

	want := map[string]string{
		"__name__":             "cpu_usage",
		"influxdb_measurement": "cpu/load",
		"influxdb_field":       "usage idle",
		"env":                  "prod",
		"host_name":            "b",
		"host_name_2":          "a",
		"_2zone":               "z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("label %s = %q, want %q (all labels: %#v)", k, got[k], v, got)
		}
	}
}

func TestParseLabelArgs(t *testing.T) {
	got, err := parseLabelArgs([]string{`cluster="prod-a"`, "tenant=acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got["cluster"] != "prod-a" || got["tenant"] != "acme" {
		t.Fatalf("unexpected labels: %#v", got)
	}
}
