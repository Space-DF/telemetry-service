package activity_logs

import (
	"encoding/json"
	"time"
)

type DeviceActivityLogRequest struct {
	ID        string          `json:"id"`
	Time      time.Time       `json:"time"`
	DeviceEUI string          `json:"device_eui"`
	Payload   json.RawMessage `json:"payload"`
}
