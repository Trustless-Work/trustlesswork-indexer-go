package health

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus surface. The collector reads the SAME Tracker snapshot that
// backs /status, so Grafana and a human mid-incident can never disagree —
// one source of truth, two renderings.

const metricsNamespace = "indexer"

// NewRegistry builds the process metrics registry with the tracker
// collector installed. Callers register additional collectors (the RPC
// pool's fetch summary) on it before serving.
func NewRegistry(t *Tracker) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newTrackerCollector(t))
	return reg
}

// MetricsHandler serves reg in the Prometheus exposition format.
func MetricsHandler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// trackerCollector projects a Status snapshot into metrics at scrape
// time. Collect-on-scrape (instead of instrumented counters sprinkled in
// the loop) keeps the hot path untouched and the numbers consistent with
// /status by construction.
type trackerCollector struct {
	t *Tracker

	currentLedger *prometheus.Desc
	ledgerAge     *prometheus.Desc
	sinceLast     *prometheus.Desc
	knownEscrows  *prometheus.Desc
	eventsTotal   *prometheus.Desc
	statesTotal   *prometheus.Desc
	gaps          *prometheus.Desc
	suppressed    *prometheus.Desc
	ready         *prometheus.Desc
	paused        *prometheus.Desc
	uptime        *prometheus.Desc
}

func newTrackerCollector(t *Tracker) *trackerCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, "", name),
			help,
			nil,
			prometheus.Labels{"network": t.network},
		)
	}
	return &trackerCollector{
		t:             t,
		currentLedger: desc("current_ledger", "Last ledger fully processed and published."),
		ledgerAge:     desc("ledger_age_seconds", "Age of the last processed ledger's close time — the freshness number."),
		sinceLast:     desc("seconds_since_last_ledger", "Seconds since the loop last processed a ledger."),
		knownEscrows:  desc("known_escrows", "Escrow contracts currently tracked."),
		eventsTotal:   desc("events_published_total", "Escrow events, deposits and withdrawals published since boot."),
		statesTotal:   desc("state_changes_published_total", "State snapshots published since boot (activity + sweep + commands)."),
		gaps:          desc("gaps_recorded", "Ledger ranges knowingly skipped and recorded as gap evidence."),
		suppressed:    desc("suppressed_removals_total", "Removals withheld because their batch failed the plausibility guard (an RPC answering with an empty result set). Anything above zero warrants looking at the RPC endpoint."),
		ready:         desc("ready", "1 when the loop is demonstrably advancing (the /readyz semantic)."),
		paused:        desc("paused", "1 while an operator pause is in effect."),
		uptime:        desc("uptime_seconds", "Process uptime."),
	}
}

func (c *trackerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.currentLedger, c.ledgerAge, c.sinceLast, c.knownEscrows,
		c.eventsTotal, c.statesTotal, c.gaps, c.suppressed, c.ready, c.paused,
		c.uptime,
	} {
		ch <- d
	}
}

func (c *trackerCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.t.Snapshot()
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}

	gauge(c.currentLedger, float64(s.CurrentLedger))
	gauge(c.ledgerAge, s.LedgerAgeSeconds)
	gauge(c.sinceLast, s.SecondsSinceLastLedger)
	gauge(c.knownEscrows, float64(s.KnownEscrows))
	counter(c.eventsTotal, float64(s.EventsPublished))
	counter(c.statesTotal, float64(s.StateChangesPublished))
	gauge(c.gaps, float64(s.Gaps))
	counter(c.suppressed, float64(s.SuppressedRemovals))
	gauge(c.ready, b2f(s.Ready))
	gauge(c.paused, b2f(s.Paused))
	gauge(c.uptime, s.UptimeSeconds)
}
