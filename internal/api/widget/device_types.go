package widget

import (
	"context"
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func gaugeHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	value, unitOfMeasurement, err := tsClient.GetLatestEntityValue(ctx, req.EntityID)
	if err != nil {
		logger.Error("failed to get latest entity value", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve value"})
	}

	return c.JSON(http.StatusOK, GaugeValueResponse{
		Value:             value,
		UnitOfMeasurement: unitOfMeasurement,
	})
}

func switchHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	boolValue, err := tsClient.GetLatestEntityBoolValue(ctx, req.EntityID)
	if err != nil {
		logger.Error("failed to get latest entity bool value", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve value"})
	}

	return c.JSON(http.StatusOK, SwitchValueResponse{Value: boolValue})
}

func chartHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	dataPoints, err := tsClient.GetAggregatedEntityData(
		ctx, req.EntityID, *req.StartTime, *req.EndTime,
	)
	if err != nil {
		logger.Error("failed to get aggregated data", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve chart data"})
	}

	chartPoints := make([]ChartDataPoint, len(dataPoints))
	for i, dp := range dataPoints {
		chartPoints[i] = ChartDataPoint{Timestamp: dp.Timestamp, Value: dp.Value}
	}

	return c.JSON(http.StatusOK, ChartDataResponse{Data: chartPoints})
}

func histogramHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	buckets, err := tsClient.GetHistogramData(
		ctx, req.EntityID, *req.StartTime, *req.EndTime,
	)
	if err != nil {
		logger.Error("failed to get histogram data", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve histogram data"})
	}

	histogramBuckets := make([]HistogramBucket, len(buckets))
	for i, b := range buckets {
		histogramBuckets[i] = HistogramBucket{Bucket: b.Bucket, Count: b.Count, Value: b.Value}
	}

	return c.JSON(http.StatusOK, HistogramDataResponse{Data: histogramBuckets})
}

func tableHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	tableData, columns, err := tsClient.GetTableData(ctx, req.EntityID, *req.StartTime, *req.EndTime)
	if err != nil {
		logger.Error("failed to get table data", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve table data"})
	}

	rows := make([]TableRow, len(tableData))
	for i, row := range tableData {
		rows[i] = TableRow{Timestamp: row.Timestamp, Values: row.Values}
	}

	return c.JSON(http.StatusOK, TableDataResponse{Columns: columns, Data: rows})
}
func mapHandler(c echo.Context, logger *zap.Logger, tsClient *timescaledb.Client, ctx context.Context, req WidgetDataRequest) error {
	latitude, longitude, err := tsClient.GetLatestEntityLocation(ctx, req.EntityID)
	if err != nil {
		logger.Error("failed to get entity location", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve location data"})
	}

	return c.JSON(http.StatusOK, MapDataResponse{
		Coordinate: Coordinate{
			Latitude:  latitude,
			Longitude: longitude,
		},
	})
}