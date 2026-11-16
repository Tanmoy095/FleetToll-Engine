package types

const LegacyTenantID int64 = 1

type Invoice struct {
	TenantID      int64   `json:"tenantID"`
	OBUID         int     `json:"obuID"`
	TotalDistance float64 `json:"totalDistance"`
	TotalAmount   float64 `json:"totalAmount"`
}

type OBUData struct {
	TenantID int64   `json:"tenantID"`
	OBUID     int     `json:"obuID"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"long"`
}

type Distance struct {
	TenantID int64   `json:"tenantID"`
	EventID string  `json:"eventID"`
	Value   float64 `json:"value"`
	OBUID   int     `json:"obuID"`
	Unix    int64   `json:"unix"` //time.Unix
}
