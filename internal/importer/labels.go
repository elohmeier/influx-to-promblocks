package importer

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/prometheus/prometheus/model/labels"
)

var labelValueRE = regexp.MustCompile(`^"?([^"]*)"?$`)

const (
	MetricNameModeMeasurementField = "measurement-field"
	MetricNameModeField            = "field"
)

func parseLabelArgs(args []string) (map[string]string, error) {
	out := make(map[string]string, len(args))
	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			return nil, fmt.Errorf("label %q must be key=value", arg)
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			return nil, fmt.Errorf("label %q has empty key", arg)
		}
		if matches := labelValueRE.FindStringSubmatch(v); len(matches) == 2 {
			v = matches[1]
		}
		out[sanitizeLabelName(k)] = v
	}
	return out, nil
}

func buildLabels(metricName, measurement, field string, tags, static map[string]string, preserveSource bool) labels.Labels {
	raw := make(map[string]string, len(tags)+len(static)+3)
	raw["__name__"] = metricName
	if preserveSource {
		raw["influxdb_measurement"] = measurement
		raw["influxdb_field"] = field
	}
	for k, v := range static {
		raw[k] = v
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	seen := make(map[string]int, len(raw)+len(tags))
	for k := range raw {
		seen[k] = 1
	}
	for _, original := range keys {
		name := sanitizeLabelName(original)
		if name == "__name__" {
			name = "influxdb_tag___name__"
		}
		if n := seen[name]; n > 0 {
			base := name
			for {
				n++
				name = fmt.Sprintf("%s_%d", base, n)
				if _, exists := seen[name]; !exists {
					break
				}
			}
		}
		seen[name] = 1
		raw[name] = tags[original]
	}
	return labels.FromMap(raw)
}

func metricName(mode, prefix, measurement, field string) string {
	mode = normalizeMetricNameMode(mode)
	base := field
	if mode == MetricNameModeMeasurementField {
		base = measurement
	}
	if mode == MetricNameModeMeasurementField && field != "value" {
		base = measurement + "_" + field
	}
	return sanitizeMetricName(prefix + base)
}

func normalizeMetricNameMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return MetricNameModeMeasurementField
	}
	return mode
}

func sanitizeMetricName(s string) string {
	if s == "" {
		return "influx_metric"
	}
	var b strings.Builder
	for _, r := range s {
		valid := r == '_' || r == ':' || unicode.IsLetter(r) || unicode.IsDigit(r)
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	first := rune(out[0])
	if !(first == '_' || first == ':' || unicode.IsLetter(first)) {
		out = "influx_" + out
	}
	return out
}

func sanitizeLabelName(s string) string {
	if s == "" {
		return "label"
	}
	var b strings.Builder
	for _, r := range s {
		valid := r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	first := rune(out[0])
	if !(first == '_' || unicode.IsLetter(first)) {
		out = "_" + out
	}
	return out
}
