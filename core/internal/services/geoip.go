package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

// ip-api.com free batch endpoint limits (https://ip-api.com/docs/api:batch):
// - Up to 100 IPs per POST body
// - 15 batch requests per minute per client IP
// - X-Rl: requests remaining; X-Ttl: seconds until window resets
// - HTTP 429 when throttled; repeated abuse → 1h ban
const (
	ipAPIBatchURL      = "http://ip-api.com/batch"
	ipAPIBatchMaxIPs   = 100
	ipAPIBatchMaxPerMin = 15
	// Minimum spacing between batch calls (60s / 15 requests).
	ipAPIBatchMinInterval = 4 * time.Second
)

// ipAPIResponse is the response from ip-api.com batch endpoint
type ipAPIResponse struct {
	Status      string  `json:"status"`
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"regionName"`
	City        string  `json:"city"`
	ISP         string  `json:"isp"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	Query       string  `json:"query"`
}

type cacheEntry struct {
	geo      models.GeoInfo
	cachedAt time.Time
}

// GeoIPService performs IP geolocation lookups via ip-api.com (free, no key needed).
// It caches results for 24 h, batches up to 100 IPs per request, and respects
// the 15 requests/minute rate limit using X-Rl / X-Ttl response headers.
type GeoIPService struct {
	client   *http.Client
	cache    map[string]cacheEntry
	mu       sync.RWMutex
	logger   *logger.Logger
	cacheTTL time.Duration

	rateMu              sync.Mutex
	requestsRemaining   int       // from X-Rl; -1 = unknown (allow cautiously)
	blockedUntil        time.Time // do not call API before this instant
	lastRequestAt       time.Time // spacing guard between calls
}

// NewGeoIPService creates a new GeoIPService
func NewGeoIPService(log *logger.Logger) *GeoIPService {
	return &GeoIPService{
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		cache:             make(map[string]cacheEntry),
		logger:            log,
		cacheTTL:          24 * time.Hour,
		requestsRemaining: -1,
	}
}

// extractIP parses "host:port" and returns just the host IP.
func extractIP(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return strings.TrimSpace(address)
	}
	return strings.TrimSpace(host)
}

// LookupOne returns GeoInfo for a single proxy address ("host:port" or bare IP).
func (g *GeoIPService) LookupOne(ctx context.Context, address string) (*models.GeoInfo, error) {
	ip := extractIP(address)
	if ip == "" {
		return nil, fmt.Errorf("empty address")
	}

	g.mu.RLock()
	if entry, ok := g.cache[ip]; ok && time.Since(entry.cachedAt) < g.cacheTTL {
		g.mu.RUnlock()
		geo := entry.geo
		return &geo, nil
	}
	g.mu.RUnlock()

	raw, err := g.lookupBatchRaw(ctx, []string{ip}, nil)
	if err != nil {
		return nil, err
	}
	geo, ok := raw[ip]
	if !ok {
		return nil, fmt.Errorf("no result for %s", ip)
	}
	return &geo, nil
}

// waitForRateLimit blocks until we may send another batch request.
func (g *GeoIPService) waitForRateLimit(ctx context.Context) error {
	for {
		g.rateMu.Lock()
		now := time.Now()

		if g.blockedUntil.After(now) {
			wait := time.Until(g.blockedUntil)
			g.rateMu.Unlock()
			g.logger.Info("geoip rate limit: waiting for window reset", "seconds", int(wait.Seconds())+1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		if g.requestsRemaining == 0 {
			wait := ipAPIBatchMinInterval
			if g.blockedUntil.After(now) {
				wait = time.Until(g.blockedUntil)
			}
			g.rateMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		if !g.lastRequestAt.IsZero() {
			elapsed := now.Sub(g.lastRequestAt)
			if elapsed < ipAPIBatchMinInterval {
				wait := ipAPIBatchMinInterval - elapsed
				g.rateMu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
		}

		g.rateMu.Unlock()
		return nil
	}
}

// updateRateLimitFromHeaders applies X-Rl and X-Ttl from an ip-api.com response.
func (g *GeoIPService) updateRateLimitFromHeaders(resp *http.Response) {
	rlHeader := resp.Header.Get("X-Rl")
	ttlHeader := resp.Header.Get("X-Ttl")

	g.rateMu.Lock()
	defer g.rateMu.Unlock()

	if rl, err := strconv.Atoi(rlHeader); err == nil {
		g.requestsRemaining = rl
		if rl == 0 {
			ttl := 60
			if t, err := strconv.Atoi(ttlHeader); err == nil && t > 0 {
				ttl = t
			}
			g.blockedUntil = time.Now().Add(time.Duration(ttl) * time.Second)
			g.logger.Warn("geoip rate limit exhausted", "retry_after_seconds", ttl)
		}
	}

	if ttl, err := strconv.Atoi(ttlHeader); err == nil && ttl > 0 {
		resetAt := time.Now().Add(time.Duration(ttl) * time.Second)
		if resetAt.After(g.blockedUntil) {
			g.blockedUntil = resetAt
		}
	}
}

// lookupBatchRaw fetches geo data for the given IPs and returns map[ip]->GeoInfo.
// Internally chunked to 100 IPs per request (ip-api.com batch limit).
func (g *GeoIPService) lookupBatchRaw(ctx context.Context, ips []string, stats *models.GeoEnrichResult) (map[string]models.GeoInfo, error) {
	if len(ips) == 0 {
		return nil, nil
	}

	result := make(map[string]models.GeoInfo, len(ips))

	for i := 0; i < len(ips); i += ipAPIBatchMaxIPs {
		if err := g.waitForRateLimit(ctx); err != nil {
			return result, err
		}

		end := i + ipAPIBatchMaxIPs
		if end > len(ips) {
			end = len(ips)
		}
		batch := ips[i:end]

		chunk, err := g.fetchBatch(ctx, batch, stats)
		if err != nil {
			if stats != nil {
				stats.RateLimited = stats.RateLimited || strings.Contains(err.Error(), "rate limit")
			}
			return result, fmt.Errorf("geoip batch %d-%d: %w", i, end, err)
		}
		for ip, geo := range chunk {
			result[ip] = geo
		}
	}
	return result, nil
}

// fetchBatch sends a single POST to ip-api.com/batch for up to 100 IPs.
func (g *GeoIPService) fetchBatch(ctx context.Context, ips []string, stats *models.GeoEnrichResult) (map[string]models.GeoInfo, error) {
	if len(ips) == 0 {
		return nil, nil
	}
	if len(ips) > ipAPIBatchMaxIPs {
		return nil, fmt.Errorf("geoip batch too large: %d (max %d)", len(ips), ipAPIBatchMaxIPs)
	}

	type reqItem struct {
		Query  string `json:"query"`
		Fields string `json:"fields"`
	}
	items := make([]reqItem, len(ips))
	fields := "status,country,countryCode,regionName,city,isp,lat,lon,query"
	for i, ip := range ips {
		items[i] = reqItem{Query: ip, Fields: fields}
	}

	body, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geoip request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ipAPIBatchURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geoip request failed: %w", err)
	}
	defer resp.Body.Close()

	g.rateMu.Lock()
	g.lastRequestAt = time.Now()
	g.rateMu.Unlock()

	if stats != nil {
		stats.BatchQueries++
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		g.updateRateLimitFromHeaders(resp)
		if stats != nil {
			stats.RateLimited = true
		}
		return nil, fmt.Errorf("geoip api rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("geoip api returned %d", resp.StatusCode)
	}

	g.updateRateLimitFromHeaders(resp)

	var responses []ipAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		return nil, fmt.Errorf("failed to decode geoip response: %w", err)
	}

	result := make(map[string]models.GeoInfo, len(responses))
	g.mu.Lock()
	for _, r := range responses {
		if r.Status != "success" {
			if stats != nil {
				stats.LookupFailed++
			}
			continue
		}
		if stats != nil {
			stats.LookupSuccess++
		}
		geo := models.GeoInfo{
			CountryCode: r.CountryCode,
			CountryName: r.Country,
			RegionName:  r.Region,
			CityName:    r.City,
			ISP:         r.ISP,
			Latitude:    r.Lat,
			Longitude:   r.Lon,
		}
		result[r.Query] = geo
		g.cache[r.Query] = cacheEntry{geo: geo, cachedAt: time.Now()}
	}
	g.mu.Unlock()
	return result, nil
}

// EnrichProxies calls ip-api.com for all addresses and returns map[address]->GeoInfo plus run stats.
func (g *GeoIPService) EnrichProxies(ctx context.Context, addresses []string) (map[string]models.GeoInfo, models.GeoEnrichResult) {
	stats := models.GeoEnrichResult{MaxIPsPerBatch: ipAPIBatchMaxIPs}
	if len(addresses) == 0 {
		return nil, stats
	}

	ipToAddr := make(map[string]string)
	for _, addr := range addresses {
		ip := extractIP(addr)
		if ip != "" {
			ipToAddr[ip] = addr
		}
	}

	ips := make([]string, 0, len(ipToAddr))
	for ip := range ipToAddr {
		ips = append(ips, ip)
	}

	result := make(map[string]models.GeoInfo)

	var needed []string
	g.mu.RLock()
	for _, ip := range ips {
		if entry, ok := g.cache[ip]; ok && time.Since(entry.cachedAt) < g.cacheTTL {
			if addr, ok2 := ipToAddr[ip]; ok2 {
				result[addr] = entry.geo
			}
		} else {
			needed = append(needed, ip)
		}
	}
	g.mu.RUnlock()

	stats.CacheHits = len(result)

	if len(needed) == 0 {
		return result, stats
	}

	raw, err := g.lookupBatchRaw(ctx, needed, &stats)
	if err != nil {
		g.logger.Warn("geoip enrichment partial or failed",
			"error", err,
			"requested", len(needed),
			"resolved", len(raw),
			"batch_queries", stats.BatchQueries,
		)
	}
	for ip, geo := range raw {
		if addr, ok := ipToAddr[ip]; ok {
			result[addr] = geo
		}
	}

	return result, stats
}
