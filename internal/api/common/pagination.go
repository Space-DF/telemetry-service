package common

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/labstack/echo/v4"
)

// DefaultLimit is the default number of items per page for most endpoints
const DefaultLimit = 20

// PaginatedResponse represents a generic paginated API response.
type PaginatedResponse struct {
	Count    int         `json:"count"`
	Next     *string     `json:"next"`
	Previous *string     `json:"previous"`
	Results  interface{} `json:"results"`
}

// PaginationParams holds parsed limit and offset values.
type PaginationParams struct {
	Limit  int
	Offset int
}

// ParsePagination extracts limit and offset from query params with defaults.
func ParsePagination(c echo.Context, defaultLimit int) PaginationParams {
	limit, _ := strconv.Atoi(c.QueryParam("limit"))
	if limit <= 0 {
		limit = defaultLimit
	}
	offset, _ := strconv.Atoi(c.QueryParam("offset"))
	if offset < 0 {
		offset = 0
	}
	return PaginationParams{Limit: limit, Offset: offset}
}

// Paginate takes a total count, pagination params, base URL built from the request, and optional
func Paginate(total int, p PaginationParams, baseURL string, extra url.Values) (next *string, previous *string) {
	end := p.Offset + p.Limit

	buildURL := func(offset int) string {
		q := url.Values{}
		q.Set("limit", strconv.Itoa(p.Limit))
		q.Set("offset", strconv.Itoa(offset))
		for k, vs := range extra {
			for _, v := range vs {
				q.Set(k, v)
			}
		}
		return fmt.Sprintf("%s?%s", baseURL, q.Encode())
	}

	if end < total {
		nextURL := buildURL(end)
		next = &nextURL
	}

	if p.Offset > 0 {
		prevOffset := p.Offset - p.Limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevURL := buildURL(prevOffset)
		previous = &prevURL
	}

	return next, previous
}

// BuildBaseURL constructs the full URL (scheme + host + path) from the request.
func BuildBaseURL(c echo.Context) string {
	scheme := c.Scheme()
	host := c.Request().Host
	path := c.Request().URL.Path
	return fmt.Sprintf("%s://%s%s", scheme, host, path)
}

// ExtraParams clones the request's query parameters, stripping limit and offset
// so they can be passed to Paginate for building next/previous URLs.
func ExtraParams(c echo.Context) url.Values {
	extra := url.Values{}
	for k, vs := range c.QueryParams() {
		if k == "limit" || k == "offset" {
			continue
		}
		for _, v := range vs {
			extra.Add(k, v)
		}
	}
	return extra
}

// SlicePage returns the slice of items for the current page.
func SlicePage(total int, p PaginationParams) (start, end int) {
	start = p.Offset
	if start > total {
		start = total
	}
	end = p.Offset + p.Limit
	if end > total {
		end = total
	}
	return start, end
}
