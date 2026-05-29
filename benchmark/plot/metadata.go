// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package plot

import "fmt"

// MetadataPreset configures a paper-oriented figure layout for namespace and
// metadata-service benchmark claims.
type MetadataPreset string

const (
	MetadataPresetNamespaceSteadyState MetadataPreset = "namespace_steady_state"
	MetadataPresetNamespacePagination  MetadataPreset = "namespace_pagination_modes"
	MetadataPresetNamespaceMixedPag    MetadataPreset = "namespace_mixed_pagination"
	MetadataPresetNamespaceDeepDesc    MetadataPreset = "namespace_deep_descendants"
	MetadataPresetNamespaceRepair      MetadataPreset = "namespace_repair_cost"
	MetadataPresetMetadataLatency      MetadataPreset = "metadata_latency"
)

// MetadataFigureConfig bundles a grouped bar chart with a domain-specific
// preset that controls title, label, and ordering.
type MetadataFigureConfig struct {
	Preset MetadataPreset
	Title  string
	YLabel string
	Output string
}

// MetadataChartConfig resolves one optional domain preset into a generic chart
// config. Callers can still override the returned fields before rendering.
func MetadataChartConfig(cfg MetadataFigureConfig) (GroupedBarChartConfig, error) {
	base := GroupedBarChartConfig{
		Output: cfg.Output,
		Theme:  DefaultTheme(),
	}
	switch cfg.Preset {
	case MetadataPresetNamespaceSteadyState:
		base.Title = fallback(cfg.Title, "Namespace Listing Steady-State Comparison")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.CategoryOrder = []string{"single-page", "paginated", "strict-paginated", "mixed-create+list"}
		base.SeriesOrder = []string{"read-plane", "secondary-index", "flat-scan"}
	case MetadataPresetNamespacePagination:
		base.Title = fallback(cfg.Title, "Steady-State Paginated Listing")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.CategoryOrder = []string{"steady-paginated"}
		base.SeriesOrder = []string{"secondary-index", "repairing-read-plane", "strict-read-plane"}
	case MetadataPresetNamespaceMixedPag:
		base.Title = fallback(cfg.Title, "Mixed Create + Paginated Listing")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.CategoryOrder = []string{"mixed-create-paginated"}
		base.SeriesOrder = []string{"secondary-index", "repairing-read-plane", "repair-then-strict"}
	case MetadataPresetNamespaceDeepDesc:
		base.Title = fallback(cfg.Title, "Deep-Descendant Direct-Children Listing")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.CategoryOrder = []string{"deep-descendants"}
		base.SeriesOrder = []string{"read-plane", "secondary-index", "flat-scan"}
	case MetadataPresetNamespaceRepair:
		base.Title = fallback(cfg.Title, "Repair and Materialization Costs")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.CategoryOrder = []string{"hot-page-fold", "hot-page-split", "cold-bootstrap", "verify", "materialize", "rebuild"}
		base.SeriesOrder = []string{"read-plane"}
	case MetadataPresetMetadataLatency:
		base.Title = fallback(cfg.Title, "Metadata Service Latency Comparison")
		base.YLabel = fallback(cfg.YLabel, "Latency (µs)")
		base.SeriesOrder = []string{"local", "raftstore", "external"}
	default:
		return GroupedBarChartConfig{}, fmt.Errorf("unsupported metadata preset %q", cfg.Preset)
	}
	return base, nil
}

// WriteMetadataFigure renders a grouped bar chart using a preset tailored to
// namespace / metadata-service evaluation.
func WriteMetadataFigure(observations []Observation, cfg MetadataFigureConfig) error {
	chartCfg, err := MetadataChartConfig(cfg)
	if err != nil {
		return err
	}
	return WriteGroupedBarChart(observations, chartCfg)
}

func fallback(value, def string) string {
	if value != "" {
		return value
	}
	return def
}
