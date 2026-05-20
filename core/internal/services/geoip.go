package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
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
// It caches results for 24 h and batches requests in groups of 100.
type GeoIPService struct {
	client   *http.Client
	cache    map[string]cacheEntry
	mu       sync.RWMutex
	logger   *logger.Logger
	cacheTTL time.Duration
}

// NewGeoIPService creates a new GeoIPService
func NewGeoIPService(log *logger.Logger) *GeoIPService {
	return &GeoIPService{
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		cache:    make(map[string]cacheEntry),
		logger:   log,
		cacheTTL: 24 * time.Hour,
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

	// Check cache first
	g.mu.RLock()
	if entry, ok := g.cache[ip]; ok && time.Since(entry.cachedAt) < g.cacheTTL {
		g.mu.RUnlock()
		geo := entry.geo
		return &geo, nil
	}
	g.mu.RUnlock()

	// lookupBatchRaw already writes to cache; fetch and return the specific IP's result.
	raw, err := g.lookupBatchRaw(ctx, []string{ip})
	if err != nil {
		return nil, err
	}
	geo, ok := raw[ip]
	if !ok {
		return nil, fmt.Errorf("no result for %s", ip)
	}
	return &geo, nil
}

// lookupBatchRaw fetches geo data for the given IPs and returns map[ip]->GeoInfo.
// Internally chunked to 100 IPs per request (ip-api.com limit). Results are cached.
func (g *GeoIPService) lookupBatchRaw(ctx context.Context, ips []string) (map[string]models.GeoInfo, error) {
	if len(ips) == 0 {
		return nil, nil
	}

	const batchSize = 100
	result := make(map[string]models.GeoInfo, len(ips))

	for i := 0; i < len(ips); i += batchSize {
		end := i + batchSize
		if end > len(ips) {
			end = len(ips)
		}
		batch := ips[i:end]

		chunk, err := g.fetchBatch(ctx, batch)
		if err != nil {
			return result, fmt.Errorf("geoip batch %d-%d: %w", i, end, err)
		}
		for ip, geo := range chunk {
			result[ip] = geo
		}
	}
	return result, nil
}

// fetchBatch sends a single POST to ip-api.com for up to 100 IPs, caches results,
// and returns map[ip]->GeoInfo.
func (g *GeoIPService) fetchBatch(ctx context.Context, ips []string) (map[string]models.GeoInfo, error) {
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://ip-api.com/batch", strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geoip request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("geoip api returned %d", resp.StatusCode)
	}

	var responses []ipAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		return nil, fmt.Errorf("failed to decode geoip response: %w", err)
	}

	result := make(map[string]models.GeoInfo, len(responses))
	g.mu.Lock()
	for _, r := range responses {
		if r.Status != "success" {
			continue
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

// EnrichProxies calls ip-api.com for all addresses and returns map[address]->GeoInfo.
func (g *GeoIPService) EnrichProxies(ctx context.Context, addresses []string) map[string]models.GeoInfo {
	if len(addresses) == 0 {
		return nil
	}

	// Deduplicate IPs
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

	// Serve from cache where possible
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

	if len(needed) == 0 {
		return result
	}

	// lookupBatchRaw handles chunking internally
	raw, err := g.lookupBatchRaw(ctx, needed)
	if err != nil {
		g.logger.Warn("geoip enrichment failed", "error", err, "ips", len(needed))
		return result
	}
	for ip, geo := range raw {
		if addr, ok := ipToAddr[ip]; ok {
			result[addr] = geo
		}
	}

	return result
}
