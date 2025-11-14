package health

import (
	"fmt"
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/health/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// handleHealth returns the overall health status
func handleHealth(healthChecker HealthChecker, tsClient *timescaledb.Client, logger *zap.Logger) echo.HandlerFunc {
	return func(c echo.Context) error {
		response := models.NewHealthResponse()
		healthy := true

		// Check AMQP connection
		if healthChecker != nil {
			if healthChecker.IsHealthy() {
				response.Checks["rabbitmq"] = models.ComponentCheck{
					Status:  "healthy",
					Message: "Connected to RabbitMQ",
				}
			} else {
				healthy = false
				response.Checks["rabbitmq"] = models.ComponentCheck{
					Status:  "unhealthy",
					Message: "Not connected to RabbitMQ",
				}
			}
		}

		// Check TimescaleDB connection
		if tsClient != nil {
			if err := tsClient.HealthCheck(); err != nil {
				healthy = false
				response.Checks["timescaledb"] = models.ComponentCheck{
					Status:  "unhealthy",
					Message: fmt.Sprintf("Database connection failed: %v", err),
				}
			} else {
				response.Checks["timescaledb"] = models.ComponentCheck{
					Status:  "healthy",
					Message: "Connected to TimescaleDB",
				}
			}
		}

		if !healthy {
			response.Status = "unhealthy"
			return c.JSON(http.StatusServiceUnavailable, response)
		}

		return c.JSON(http.StatusOK, response)
	}
}

// handleReady returns the readiness status
func handleReady(healthChecker HealthChecker, tsClient *timescaledb.Client, logger *zap.Logger) echo.HandlerFunc {
	return func(c echo.Context) error {
		ready := true
		message := "Service is ready"

		// Check AMQP
		if healthChecker != nil && !healthChecker.IsHealthy() {
			ready = false
			message = "AMQP not connected"
		}

		// Check TimescaleDB
		if tsClient != nil {
			if err := tsClient.HealthCheck(); err != nil {
				ready = false
				message = "TimescaleDB not connected"
			}
		}

		response := models.NewReadyResponse(ready, message)

		if !ready {
			return c.JSON(http.StatusServiceUnavailable, response)
		}

		return c.JSON(http.StatusOK, response)
	}
}

// handleLive returns the liveness status
func handleLive(logger *zap.Logger) echo.HandlerFunc {
	return func(c echo.Context) error {
		response := models.NewLiveResponse()
		return c.JSON(http.StatusOK, response)
	}
}
