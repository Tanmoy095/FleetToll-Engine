package types

type Invoice struct {
	OBUID         int     `json:"obuID"`
	TotalDistance float64 `json:"totalDistance"`
	TotalAmount   float64 `json:"totalAmount"`
}

type OBUData struct {
	OBUID     int     `json:"obuID"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"long"`
}

type Distance struct {
	EventID string  `json:"eventID"`
	Value   float64 `json:"value"`
	OBUID   int     `json:"obuID"`
	Unix    int64   `json:"unix"` //time.Unix
}
