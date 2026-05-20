package models

import "time"

// ProxySource represents a remote URL that provides a list of proxies
type ProxySource struct {
	ID              int        `json:"id"`
	Name            string     `json:"name"`
	URL             string     `json:"url"`
	Protocol        string     `json:"protocol"`
	Enabled         bool       `json:"enabled"`
	IntervalMinutes int        `json:"interval_minutes"`
	LastFetchedAt   *time.Time `json:"last_fetched_at,omitempty"`
	LastCount       int        `json:"last_count"`        // newly imported on last fetch
	LastTotal       int        `json:"last_total"`        // total lines returned on last fetch
	LastError       *string    `json:"last_error,omitempty"`
	CleanupEnabled  bool       `json:"cleanup_enabled"`   // per-source opt-in soft cleanup
	CleanupDays     int        `json:"cleanup_days"`      // delete proxies stale for this many days
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// CreateProxySourceRequest is the payload for creating a source
type CreateProxySourceRequest struct {
	Name            string `json:"name"     validate:"required"`
	URL             string `json:"url"      validate:"required,url"`
	Protocol        string `json:"protocol" validate:"required,oneof=http https socks4 socks4a socks5"`
	Enabled         bool   `json:"enabled"`
	IntervalMinutes int    `json:"interval_minutes" validate:"min=1"`
	CleanupEnabled  bool   `json:"cleanup_enabled"`
	CleanupDays     int    `json:"cleanup_days" validate:"omitempty,min=1,max=365"`
}

// GeoEnrichResult summarizes one manual or scheduled GeoIP enrichment run.
type GeoEnrichResult struct {
	Attempted       int  `json:"attempted"`        // proxy rows processed this run (max 500 per click)
	Enriched        int  `json:"enriched"`         // rows written with new geo data in DB
	Remaining       int  `json:"remaining"`        // proxies still missing geo after this run
	TotalPending    int  `json:"total_pending"`    // proxies missing geo before this run
	BatchQueries    int  `json:"batch_queries"`    // HTTP POSTs to ip-api.com/batch
	MaxIPsPerBatch  int  `json:"max_ips_per_batch"` // ip-api limit per request (100)
	LookupSuccess   int  `json:"lookup_success"`   // ip-api status=success (API responses only)
	LookupFailed    int  `json:"lookup_failed"`    // ip-api status=fail (API responses only)
	CacheHits       int  `json:"cache_hits"`       // resolved from in-memory cache, no API call
	RateLimited     bool `json:"rate_limited"`     // stopped early due to 429 / X-Rl=0
}

// UpdateProxySourceRequest is the payload for updating a source
type UpdateProxySourceRequest struct {
	Name            string `json:"name"`
	URL             string `json:"url"`
	Protocol        string `json:"protocol" validate:"omitempty,oneof=http https socks4 socks4a socks5"`
	Enabled         *bool  `json:"enabled"`
	IntervalMinutes int    `json:"interval_minutes" validate:"omitempty,min=1"`
	CleanupEnabled  *bool  `json:"cleanup_enabled"`
	CleanupDays     int    `json:"cleanup_days" validate:"omitempty,min=1,max=365"`
}
