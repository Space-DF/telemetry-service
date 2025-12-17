package timescaledb

import (
	"context"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb/alerts"
	"github.com/Space-DF/telemetry-service/internal/timescaledb/core"
	"github.com/Space-DF/telemetry-service/internal/timescaledb/read"
	"github.com/Space-DF/telemetry-service/internal/timescaledb/write"
	"go.uber.org/zap"
)

type (
	Location            = read.Location
	EntityDataPoint     = read.EntityDataPoint
	HistogramBucketData = read.HistogramBucketData
	TableDataRow        = read.TableDataRow
	ErrDroppedBatch     = core.ErrDroppedBatch
)

const (
	BatchChannelBufferSize = core.BatchChannelBufferSize
	DropTimeout            = core.DropTimeout
)

var (
	ErrLocationDroppedTimeout = core.ErrLocationDroppedTimeout
	ErrDateRequired           = core.ErrDateRequired
	ErrInvalidDateFormat      = core.ErrInvalidDateFormat
)

type Client struct {
	base   *core.Base
	Read   *read.Service
	Alerts *alerts.Service
	Ingest *write.Service
}

func NewClient(connStr string, batchSize int, flushInterval time.Duration, logger *zap.Logger) (*Client, error) {
	base, err := core.NewBase(connStr, batchSize, flushInterval, logger)
	if err != nil {
		return nil, err
	}

	return &Client{
		base:   base,
		Read:   read.NewService(base),
		Alerts: alerts.NewService(base),
		Ingest: write.NewService(base),
	}, nil
}

// Delegating API: write

func (c *Client) SaveTelemetryPayload(ctx context.Context, payload *models.TelemetryPayload) error {
	return c.Ingest.SaveTelemetryPayload(ctx, payload)
}

// Delegating API: reads

func (c *Client) GetLocationHistory(ctx context.Context, deviceID, spaceSlug string, start, end time.Time, limit int) ([]*Location, error) {
	return c.Read.GetLocationHistory(ctx, deviceID, spaceSlug, start, end, limit)
}

func (c *Client) GetLastLocation(ctx context.Context, deviceID, spaceSlug string) (*Location, error) {
	return c.Read.GetLastLocation(ctx, deviceID, spaceSlug)
}

func (c *Client) GetDeviceProperties(ctx context.Context, deviceID, spaceSlug string) (map[string]interface{}, error) {
	return c.Read.GetDeviceProperties(ctx, deviceID, spaceSlug)
}

func (c *Client) GetEntities(ctx context.Context, spaceSlug, category, deviceID string, displayTypes []string, search string, page, pageSize int) ([]map[string]interface{}, int, error) {
	return c.Read.GetEntities(ctx, spaceSlug, category, deviceID, displayTypes, search, page, pageSize)
}

func (c *Client) GetLatestAttributesForDeviceAt(ctx context.Context, deviceID string, at time.Time) (map[string]interface{}, error) {
	return c.Read.GetLatestAttributesForDeviceAt(ctx, deviceID, at)
}

func (c *Client) GetLatestEntityValue(ctx context.Context, entityID string) (float64, string, error) {
	return c.Read.GetLatestEntityValue(ctx, entityID)
}

func (c *Client) GetLatestEntityBoolValue(ctx context.Context, entityID string) (bool, error) {
	return c.Read.GetLatestEntityBoolValue(ctx, entityID)
}

func (c *Client) GetAggregatedEntityData(ctx context.Context, entityID string, startTime, endTime time.Time, groupBy string) ([]EntityDataPoint, error) {
	return c.Read.GetAggregatedEntityData(ctx, entityID, startTime, endTime, groupBy)
}

func (c *Client) GetHistogramData(ctx context.Context, entityID string, startTime, endTime time.Time, groupBy string) ([]HistogramBucketData, error) {
	return c.Read.GetHistogramData(ctx, entityID, startTime, endTime, groupBy)
}

func (c *Client) GetTableData(ctx context.Context, entityID string, startTime, endTime time.Time) ([]TableDataRow, []string, error) {
	return c.Read.GetTableData(ctx, entityID, startTime, endTime)
}

// Delegating API: alerts

func (c *Client) GetAlerts(ctx context.Context, orgSlug, category, spaceSlug, deviceID, dateStr string, cautionThreshold, warningThreshold, criticalThreshold float64, page, pageSize int) ([]interface{}, int, error) {
	return c.Alerts.GetAlerts(ctx, orgSlug, category, spaceSlug, deviceID, dateStr, cautionThreshold, warningThreshold, criticalThreshold, page, pageSize)
}

// Delegating API: schema / infra

func (c *Client) CreateSchema(ctx context.Context, orgSlug string) error {
	return c.base.CreateSchema(ctx, orgSlug)
}

func (c *Client) CreateSchemaAndTables(ctx context.Context, orgSlug string) error {
	return c.base.CreateSchemaAndTables(ctx, orgSlug)
}

func (c *Client) DropSchema(ctx context.Context, orgSlug string) error {
	return c.base.DropSchema(ctx, orgSlug)
}

func (c *Client) HealthCheck() error {
	return c.base.HealthCheck()
}

func (c *Client) Wait() {
	c.base.Wait()
}

func (c *Client) Close() error {
	return c.base.Close()
}

// Context helpers re-exported

func ContextWithOrg(ctx context.Context, org string) context.Context {
	return core.ContextWithOrg(ctx, org)
}

func OrgFromContext(ctx context.Context) string {
	return core.OrgFromContext(ctx)
}
