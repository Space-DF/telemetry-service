package models

import (
	"encoding/json"
	"time"
)

type DeviceActivityLog struct {
	ID        string          `json:"id" db:"id"`
	Time      time.Time       `json:"time" db:"time"`
	DeviceEUI string          `json:"device_eui" db:"device_eui"`
	Payload   json.RawMessage `json:"payload" db:"payload"`
}
