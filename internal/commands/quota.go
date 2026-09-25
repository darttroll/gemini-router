package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/quota"
	"github.com/darttroll/gemini-router/internal/storage"
)

type quotaColumn struct {
	GroupKey  string
	GroupName string
	Window    string
}

type QuotaJSONBucket struct {
	GroupKey          string  `json:"group_key"`
	GroupName         string  `json:"group_name"`
	Window            string  `json:"window"`
	RemainingFraction float64 `json:"remaining_fraction"`
	Disabled          bool    `json:"disabled"`
	ResetAt           string  `json:"reset_at,omitempty"`
	RefreshedAt       string  `json:"refreshed_at"`
}

type QuotaJSONProvider struct {
	Worker       string            `json:"worker"`
	RefreshError string            `json:"refresh_error,omitempty"`
	Quotas       []QuotaJSONBucket `json:"quotas"`
}

type QuotaJSONReport struct {
	Providers []QuotaJSONProvider `json:"providers"`
}

type QuotaCommand struct {
	cfg      *config.Config
	store    *storage.Store
	quotaSvc *quota.Service
}

func NewQuotaCommand(cfg *config.Config, store *storage.Store) *QuotaCommand {
	c := &QuotaCommand{cfg: cfg, store: store}
	if cfg != nil && cfg.Quota.Enabled {
		c.quotaSvc = quota.NewService(cfg, store)
	}
	return c
}

func (c *QuotaCommand) Run(w io.Writer, jsonOutput bool) error {
	refreshErrors := map[string]error{}
	if c.quotaSvc != nil {
		timeout := c.cfg.Quota.CommandTimeout + 2*time.Second
		if timeout <= 0 {
			timeout = 12 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		refreshErrors = c.quotaSvc.EnsureFreshAllGroups(ctx)
		cancel()
	}

	rowsByWorker := make(map[string][]storage.QuotaSnapshot, len(c.cfg.Workers))
	for _, worker := range c.cfg.Workers {
		rows, err := c.store.GetQuotaSnapshots(worker.Username, "")
		if err != nil {
			return fmt.Errorf("getting quota for %s: %w", worker.Username, err)
		}
		rowsByWorker[worker.Username] = rows
	}

	columns := quotaColumns(rowsByWorker)
	if jsonOutput {
		return c.writeJSON(w, rowsByWorker, columns, refreshErrors)
	}
	return c.writeTable(w, rowsByWorker, columns, refreshErrors)
}

func (c *QuotaCommand) writeTable(w io.Writer, rowsByWorker map[string][]storage.QuotaSnapshot, columns []quotaColumn, refreshErrors map[string]error) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprint(tw, "Worker")
	for _, col := range columns {
		fmt.Fprintf(tw, "\t%s", quotaColumnLabel(col))
	}
	fmt.Fprintln(tw)
	fmt.Fprint(tw, "------")
	for _, col := range columns {
		fmt.Fprintf(tw, "\t%s", strings.Repeat("-", max(3, len(quotaColumnLabel(col)))))
	}
	fmt.Fprintln(tw)

	now := time.Now().UTC()
	for _, worker := range c.cfg.Workers {
		fmt.Fprint(tw, worker.Username)
		byKey := quotaRowIndex(rowsByWorker[worker.Username])
		telemetryDisabled := !worker.Enabled || !c.cfg.QuotaEnabledFor(worker.Username)
		for _, col := range columns {
			cell := "unavailable"
			if telemetryDisabled {
				cell = "disabled"
			} else if row, ok := byKey[quotaKey(col.GroupKey, col.Window)]; ok {
				cell = formatQuotaCell(row, now)
			}
			fmt.Fprintf(tw, "\t%s", cell)
		}
		fmt.Fprintln(tw)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(refreshErrors) > 0 {
		fmt.Fprintln(w, "\nRefresh errors:")
		for _, worker := range c.cfg.Workers {
			if err, ok := refreshErrors[worker.Username]; ok {
				fmt.Fprintf(w, "%s: %v\n", worker.Username, err)
			}
		}
	}
	return nil
}

func (c *QuotaCommand) writeJSON(w io.Writer, rowsByWorker map[string][]storage.QuotaSnapshot, columns []quotaColumn, refreshErrors map[string]error) error {
	report := QuotaJSONReport{Providers: make([]QuotaJSONProvider, 0, len(c.cfg.Workers))}
	for _, worker := range c.cfg.Workers {
		provider := QuotaJSONProvider{Worker: worker.Username, Quotas: make([]QuotaJSONBucket, 0, len(rowsByWorker[worker.Username]))}
		if err, ok := refreshErrors[worker.Username]; ok {
			provider.RefreshError = err.Error()
		}
		byKey := quotaRowIndex(rowsByWorker[worker.Username])
		for _, col := range columns {
			row, ok := byKey[quotaKey(col.GroupKey, col.Window)]
			if !ok {
				continue
			}
			bucket := QuotaJSONBucket{
				GroupKey:          row.GroupKey,
				GroupName:         row.GroupName,
				Window:            row.Window,
				RemainingFraction: row.RemainingFraction,
				Disabled:          row.Disabled,
				RefreshedAt:       row.FetchedAt.Format(time.RFC3339Nano),
			}
			if !row.ResetTime.IsZero() {
				bucket.ResetAt = row.ResetTime.Format(time.RFC3339Nano)
			}
			provider.Quotas = append(provider.Quotas, bucket)
		}
		report.Providers = append(report.Providers, provider)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func quotaColumns(rowsByWorker map[string][]storage.QuotaSnapshot) []quotaColumn {
	unique := map[string]quotaColumn{}
	for _, rows := range rowsByWorker {
		for _, row := range rows {
			key := quotaKey(row.GroupKey, row.Window)
			col, exists := unique[key]
			if !exists || (row.GroupName != "" && (col.GroupName == "" || row.GroupName < col.GroupName)) {
				unique[key] = quotaColumn{GroupKey: row.GroupKey, GroupName: row.GroupName, Window: row.Window}
			}
		}
	}
	cols := make([]quotaColumn, 0, len(unique))
	for _, col := range unique {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool {
		gi, gj := quotaGroupSortKey(cols[i]), quotaGroupSortKey(cols[j])
		if gi != gj {
			return gi < gj
		}
		if cols[i].GroupKey != cols[j].GroupKey {
			return cols[i].GroupKey < cols[j].GroupKey
		}
		wi, wj := quotaWindowSortKey(cols[i].Window), quotaWindowSortKey(cols[j].Window)
		if wi != wj {
			return wi < wj
		}
		return cols[i].Window < cols[j].Window
	})
	return cols
}

func quotaGroupSortKey(c quotaColumn) string {
	switch c.GroupKey {
	case "gemini":
		return "0"
	case "third_party":
		return "1"
	default:
		return "2:" + strings.ToLower(quotaGroupDisplayName(c))
	}
}

func quotaWindowSortKey(window string) string {
	switch strings.ToLower(window) {
	case "5h":
		return "0"
	case "weekly":
		return "1"
	default:
		return "2:" + strings.ToLower(window)
	}
}

func quotaColumnLabel(c quotaColumn) string {
	window := c.Window
	if strings.EqualFold(window, "weekly") {
		window = "Weekly"
	} else if strings.EqualFold(window, "5h") {
		window = "5h"
	}
	return quotaGroupDisplayName(c) + " " + window
}

func quotaGroupDisplayName(c quotaColumn) string {
	switch c.GroupKey {
	case "gemini":
		return "Gemini"
	case "third_party":
		return "Claude/GPT"
	}
	if c.GroupName != "" {
		return c.GroupName
	}
	return c.GroupKey
}

func formatQuotaCell(row storage.QuotaSnapshot, now time.Time) string {
	if row.Disabled {
		return "disabled"
	}
	cell := fmt.Sprintf("%.1f%%", row.RemainingFraction*100)
	if !row.ResetTime.IsZero() {
		cell += " (reset " + formatUntil(row.ResetTime, now) + ")"
	}
	return cell
}

func quotaRowIndex(rows []storage.QuotaSnapshot) map[string]storage.QuotaSnapshot {
	out := make(map[string]storage.QuotaSnapshot, len(rows))
	for _, row := range rows {
		out[quotaKey(row.GroupKey, row.Window)] = row
	}
	return out
}

func quotaKey(group, window string) string {
	return group + "\x00" + window
}
