package models

import (
	"encoding/json"
	"time"
)

type DeviceActivityLog struct {
	ID        string          `json:"id" db:"id"`
	Timestamp time.Time       `json:"timestamp" db:"timestamp"`
	DeviceEUI string          `json:"device_eui" db:"device_eui"`
	Payload   json.RawMessage `json:"original_payload" db:"payload"`
}
