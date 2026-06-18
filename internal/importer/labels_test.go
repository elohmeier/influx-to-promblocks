package importer

import "testing"

func TestMetricName(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		measurement string
		field       string
		want        string
	}{
		{name: "value field uses measurement", measurement: "cpu", field: "value", want: "cpu"},
		{name: "non value field appends field", measurement: "cpu", field: "usage idle", want: "cpu_usage_idle"},
		{name: "invalid leading digit", measurement: "1cpu", field: "value", want: "influx_1cpu"},
		{name: "prefix", prefix: "legacy_", measurement: "cpu", field: "load-1", want: "legacy_cpu_load_1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metricName(tt.prefix, tt.measurement, tt.field); got != tt.want {
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
