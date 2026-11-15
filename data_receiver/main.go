package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const maxOBUMessageBytes = 4096

func main() {
	recv, err := NewDataReceiver()
	if err != nil {
		logrus.Fatal(err)
	}
	server := &http.Server{
		Addr:              ":30000",
		Handler:           newWebSocketHandler(recv),
		ReadHeaderTimeout: 5 * time.Second,
	}
	logrus.Fatal(server.ListenAndServe())
}

func newWebSocketHandler(receiver *DataReceiver) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", receiver.handleWS)
	return mux
}

// DataReceiver handles each WebSocket connection in its request goroutine.
// The producer is shared; the socket and decoder state are connection-local.
type DataReceiver struct {
	prod DataProducer
}

func NewDataReceiver() (*DataReceiver, error) {
	producer, err := NewKafkaProducer(kafkaTopicFromEnv())
	if err != nil {
		return nil, err
	}
	return newDataReceiver(NewLogMiddleware(producer)), nil
}

func newDataReceiver(producer DataProducer) *DataReceiver {
	return &DataReceiver{prod: producer}
}

func (dr *DataReceiver) produceData(data types.OBUData) error {
	return dr.prod.ProduceData(data)
}

func (dr *DataReceiver) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1028,
		WriteBufferSize: 1028,
		CheckOrigin:     checkWebSocketOrigin,
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxOBUMessageBytes)

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code != websocket.CloseNormalClosure && closeErr.Code != websocket.CloseGoingAway {
				logrus.WithError(err).Debug("WebSocket read ended")
			}
			return
		}
		if messageType != websocket.TextMessage {
			writeWebSocketClose(conn, websocket.CloseUnsupportedData, "text JSON messages are required")
			return
		}

		data, err := decodeOBUData(payload)
		if err != nil {
			writeWebSocketClose(conn, websocket.CloseInvalidFramePayloadData, "invalid telemetry payload")
			return
		}
		if err := validateOBUData(data); err != nil {
			writeWebSocketClose(conn, websocket.ClosePolicyViolation, "invalid telemetry values")
			return
		}
		if err := dr.produceData(data); err != nil {
			logrus.WithError(err).Error("could not publish telemetry")
			writeWebSocketClose(conn, websocket.CloseInternalServerErr, "telemetry temporarily unavailable")
			return
		}
		logrus.Debug("telemetry accepted")
	}
}

func decodeOBUData(payload []byte) (types.OBUData, error) {
	var input struct {
		OBUID     *int     `json:"obuID"`
		Latitude  *float64 `json:"lat"`
		Longitude *float64 `json:"long"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return types.OBUData{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return types.OBUData{}, errors.New("multiple JSON values")
		}
		return types.OBUData{}, err
	}
	if input.OBUID == nil || input.Latitude == nil || input.Longitude == nil {
		return types.OBUData{}, errors.New("obuID, lat and long are required")
	}
	return types.OBUData{OBUID: *input.OBUID, Latitude: *input.Latitude, Longitude: *input.Longitude}, nil
}

func validateOBUData(data types.OBUData) error {
	if data.OBUID <= 0 {
		return errors.New("OBU ID must be positive")
	}
	if math.IsNaN(data.Latitude) || math.IsInf(data.Latitude, 0) || data.Latitude < -90 || data.Latitude > 90 {
		return errors.New("latitude is outside [-90, 90]")
	}
	if math.IsNaN(data.Longitude) || math.IsInf(data.Longitude, 0) || data.Longitude < -180 || data.Longitude > 180 {
		return errors.New("longitude is outside [-180, 180]")
	}
	return nil
}

func checkWebSocketOrigin(r *http.Request) bool {
	originValue := r.Header.Get("Origin")
	if originValue == "" {
		return true
	}
	origin, err := url.Parse(originValue)
	if err != nil || origin.Host == "" || !strings.EqualFold(origin.Host, r.Host) {
		return false
	}
	return (r.TLS == nil && strings.EqualFold(origin.Scheme, "http")) ||
		(r.TLS != nil && strings.EqualFold(origin.Scheme, "https"))
}

func writeWebSocketClose(conn *websocket.Conn, code int, reason string) {
	deadline := time.Now().Add(time.Second)
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
}
