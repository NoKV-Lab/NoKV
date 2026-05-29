// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	benchplot "github.com/feichai0017/NoKV/benchmark/plot"
	"gonum.org/v1/plot/vg"
)

func main() {
	var (
		input         = flag.String("input", "", "input CSV path")
		output        = flag.String("output", "", "output figure path (.svg/.pdf/.png)")
		format        = flag.String("format", "observations", "input format: observations")
		title         = flag.String("title", "", "plot title")
		xlabel        = flag.String("xlabel", "", "x-axis label override")
		ylabel        = flag.String("ylabel", "", "y-axis label override")
		preset        = flag.String("preset", "", "optional metadata preset shortcut")
		config        = flag.String("config", "", "optional chart-config CSV (key,value...)")
		categoryOrder = flag.String("category-order", "", "comma-separated category order override")
		seriesOrder   = flag.String("series-order", "", "comma-separated series order override")
		categoryCol   = flag.String("category-col", "", "observation CSV category column override")
		seriesCol     = flag.String("series-col", "", "observation CSV series column override")
		valueCol      = flag.String("value-col", "", "observation CSV value column override")
		widthIn       = flag.Float64("width-in", 0, "figure width in inches")
		heightIn      = flag.Float64("height-in", 0, "figure height in inches")
		ymin          = flag.Float64("ymin", 0, "y-axis minimum override")
		hideLegend    = flag.Bool("hide-legend", false, "hide legend")
		noGrid        = flag.Bool("no-grid", false, "disable grid")
	)
	flag.Parse()

	if *input == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "plotbench: -input and -output are required")
		os.Exit(2)
	}

	var err error
	plotCfg, err := loadPlotConfig(*config, *output, *title, *xlabel, *ylabel, *categoryOrder, *seriesOrder, *categoryCol, *seriesCol, *valueCol, *widthIn, *heightIn, *ymin, *hideLegend, *noGrid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plotbench: %v\n", err)
		os.Exit(1)
	}
	switch *format {
	case "observations":
		err = renderObservations(*input, *preset, plotCfg)
	default:
		err = fmt.Errorf("unsupported format %q", *format)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "plotbench: %v\n", err)
		os.Exit(1)
	}
}

func renderObservations(input, preset string, cfg benchplot.PlotConfig) error {
	obs, err := benchplot.ReadObservationsCSVWithConfig(input, cfg.Observations)
	if err != nil {
		return err
	}
	if preset != "" {
		presetCfg, err := benchplot.MetadataChartConfig(benchplot.MetadataFigureConfig{
			Preset: benchplot.MetadataPreset(preset),
			Title:  cfg.Chart.Title,
			YLabel: cfg.Chart.YLabel,
			Output: cfg.Chart.Output,
		})
		if err != nil {
			return err
		}
		if cfg.Chart.XLabel != "" {
			presetCfg.XLabel = cfg.Chart.XLabel
		}
		if len(cfg.Chart.CategoryOrder) > 0 {
			presetCfg.CategoryOrder = cfg.Chart.CategoryOrder
		}
		if len(cfg.Chart.SeriesOrder) > 0 {
			presetCfg.SeriesOrder = cfg.Chart.SeriesOrder
		}
		if cfg.Chart.Width > 0 {
			presetCfg.Width = cfg.Chart.Width
		}
		if cfg.Chart.Height > 0 {
			presetCfg.Height = cfg.Chart.Height
		}
		presetCfg.HideLegend = cfg.Chart.HideLegend
		if cfg.Chart.GridConfigured {
			presetCfg.ShowGrid = cfg.Chart.ShowGrid
			presetCfg.GridConfigured = true
		}
		return benchplot.WriteGroupedBarChart(obs, presetCfg)
	}
	return benchplot.WriteGroupedBarChart(obs, cfg.Chart)
}

func loadPlotConfig(configPath, output, title, xlabel, ylabel, categoryOrder, seriesOrder, categoryCol, seriesCol, valueCol string, widthIn, heightIn, ymin float64, hideLegend, noGrid bool) (benchplot.PlotConfig, error) {
	cfg := benchplot.PlotConfig{
		Chart: benchplot.GroupedBarChartConfig{
			Output: output,
			Theme:  benchplot.DefaultTheme(),
		},
		Observations: benchplot.DefaultObservationCSVConfig(),
	}
	if configPath != "" {
		fileCfg, err := benchplot.ReadPlotConfigCSV(configPath)
		if err != nil {
			return benchplot.PlotConfig{}, err
		}
		cfg = fileCfg
		cfg.Chart.Output = output
		if len(cfg.Chart.Theme.Palette) == 0 {
			cfg.Chart.Theme = benchplot.DefaultTheme()
		}
	}
	if title != "" {
		cfg.Chart.Title = title
	}
	if xlabel != "" {
		cfg.Chart.XLabel = xlabel
	}
	if ylabel != "" {
		cfg.Chart.YLabel = ylabel
	}
	if categoryOrder != "" {
		cfg.Chart.CategoryOrder = splitCSVList(categoryOrder)
	}
	if seriesOrder != "" {
		cfg.Chart.SeriesOrder = splitCSVList(seriesOrder)
	}
	if categoryCol != "" {
		cfg.Observations.CategoryColumn = categoryCol
	}
	if seriesCol != "" {
		cfg.Observations.SeriesColumn = seriesCol
	}
	if valueCol != "" {
		cfg.Observations.ValueColumn = valueCol
	}
	if widthIn > 0 {
		cfg.Chart.Width = vg.Length(widthIn) * vg.Inch
	}
	if heightIn > 0 {
		cfg.Chart.Height = vg.Length(heightIn) * vg.Inch
	}
	if ymin != 0 {
		cfg.Chart.YMin = ymin
	}
	if hideLegend {
		cfg.Chart.HideLegend = true
	}
	if noGrid {
		cfg.Chart.ShowGrid = false
		cfg.Chart.GridConfigured = true
	}
	cfg.Observations = benchplot.NormalizeObservationCSVConfig(cfg.Observations)
	return cfg, nil
}

func splitCSVList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
