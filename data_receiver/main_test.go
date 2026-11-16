package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type recordingProducer struct {
	items chan types.OBUData
}

func TestWebSocketAuthenticatesTenantAndScopesOBU(t *testing.T) {
	credentials, err := tenantauth.New([]tenantauth.CredentialSpec{
		{Token: "device-tenant-one-0123456789abcdef012345", TenantID: 1, Role: tenantauth.RoleDevice, OBUIDs: []int{7}},
		{Token: "device-tenant-two-0123456789abcdef012345", TenantID: 2, Role: tenantauth.RoleDevice, OBUIDs: []int{7}},
	})
	if err != nil {
		t.Fatal(err)
	}
	producer := newRecordingProducer()
	server := httptest.NewServer(newWebSocketHandler(newDataReceiver(producer, credentials)))
	defer server.Close()

	_, response, err := websocket.DefaultDialer.Dial(websocketURL(server.URL), nil)
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated upgrade err=%v status=%v, want 401", err, response)
	}

	for _, tc := range []struct {
		token string
		body  string
		want  int64
	}{
		{"device-tenant-one-0123456789abcdef012345", `{"obuID":7,"lat":0,"long":0}`, 1},
		{"device-tenant-two-0123456789abcdef012345", `{"obuID":7,"lat":0,"long":0}`, 2},
	} {
		header := http.Header{"Authorization": []string{"Bearer " + tc.token}}
		conn := dialWebSocket(t, server.URL, header)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(tc.body)); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-producer.items:
			if got.TenantID != tc.want || got.OBUID != 7 {
				t.Fatalf("published telemetry = %+v, want tenant %d OBU 7", got, tc.want)
			}
		case <-time.After(time.Second):
			t.Fatal("authorized telemetry was not published")
		}
		conn.Close()
	}

	header := http.Header{"Authorization": []string{"Bearer device-tenant-one-0123456789abcdef012345"}}
	conn := dialWebSocket(t, server.URL, header)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"obuID":8,"lat":0,"long":0}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("device credential published an OBU outside its scope")
	}
	select {
	case got := <-producer.items:
		t.Fatalf("out-of-scope telemetry reached producer: %+v", got)
	default:
	}
}

func newRecordingProducer() *recordingProducer {
	return &recordingProducer{items: make(chan types.OBUData, 16)}
}

func newTestAuthenticator(t *testing.T) tenantauth.Authenticator {
	t.Helper()
	auth, err := tenantauth.New([]tenantauth.CredentialSpec{{
		Token: "test-device-token-0123456789abcdef0123456789", TenantID: 1,
		Role: tenantauth.RoleDevice, OBUIDs: []int{7, 8, 9},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func newTestDataReceiver(t *testing.T, producer DataProducer) *DataReceiver {
	t.Helper()
	return newDataReceiver(producer, newTestAuthenticator(t))
}

func (p *recordingProducer) ProduceData(data types.OBUData) error {
	p.items <- data
	return nil
}

func TestWebSocketRejectsInvalidCoordinates(t *testing.T) {
	producer := newRecordingProducer()
	receiver := newTestDataReceiver(t, producer)
	server := httptest.NewServer(newWebSocketHandler(receiver))
	defer server.Close()

	conn := dialWebSocket(t, server.URL, nil)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"obuID":7,"lat":91,"long":0}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("invalid coordinates should close the WebSocket")
	}
	select {
	case got := <-producer.items:
		t.Fatalf("invalid telemetry reached producer: %+v", got)
	default:
	}
}

func TestWebSocketRejectsMissingCoordinateFields(t *testing.T) {
	producer := newRecordingProducer()
	server := httptest.NewServer(newWebSocketHandler(newTestDataReceiver(t, producer)))
	defer server.Close()
	conn := dialWebSocket(t, server.URL, nil)
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"obuID":7,"lat":51.2}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("payload with a missing longitude should close the WebSocket")
	}
	select {
	case got := <-producer.items:
		t.Fatalf("incomplete telemetry reached producer: %+v", got)
	default:
	}
}

func TestWebSocketPublishesValidTelemetryAndHandlesMultipleConnections(t *testing.T) {
	producer := newRecordingProducer()
	receiver := newTestDataReceiver(t, producer)
	server := httptest.NewServer(newWebSocketHandler(receiver))
	defer server.Close()

	first := dialWebSocket(t, server.URL, nil)
	defer first.Close()
	second := dialWebSocket(t, server.URL, nil)
	defer second.Close()

	for _, event := range []struct {
		conn    *websocket.Conn
		payload string
	}{
		{conn: first, payload: `{"obuID":7,"lat":51.2,"long":4.4}`},
		{conn: second, payload: `{"obuID":8,"lat":40.7,"long":-74.0}`},
	} {
		if err := event.conn.WriteMessage(websocket.TextMessage, []byte(event.payload)); err != nil {
			t.Fatal(err)
		}
	}

	gotIDs := map[int]bool{}
	for len(gotIDs) < 2 {
		select {
		case data := <-producer.items:
			gotIDs[data.OBUID] = true
		case <-time.After(time.Second):
			t.Fatalf("expected messages from both connections; received OBUs %v", gotIDs)
		}
	}
	if !gotIDs[7] || !gotIDs[8] {
		t.Errorf("records were routed incorrectly: %v", gotIDs)
	}
}

func TestWebSocketRejectsUnknownFieldsAndOversizedPayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "unknown field", payload: `{"obuID":9,"lat":1,"long":2,"tenantID":5}`},
		{name: "oversized frame", payload: `{"obuID":9,"lat":1,"long":2,"padding":"` + strings.Repeat("x", 8192) + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := newRecordingProducer()
			receiver := newTestDataReceiver(t, producer)
			server := httptest.NewServer(newWebSocketHandler(receiver))
			defer server.Close()
			conn := dialWebSocket(t, server.URL, nil)
			defer conn.Close()
			if err := conn.WriteMessage(websocket.TextMessage, []byte(tt.payload)); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, _, err := conn.ReadMessage()
			if err == nil {
				t.Fatal("invalid frame should close the WebSocket")
			}
			select {
			case got := <-producer.items:
				t.Fatalf("invalid telemetry reached producer: %+v", got)
			default:
			}
		})
	}
}

func TestWebSocketRejectsCrossOriginUpgrade(t *testing.T) {
	producer := newRecordingProducer()
	receiver := newTestDataReceiver(t, producer)
	server := httptest.NewServer(newWebSocketHandler(receiver))
	defer server.Close()

	header := http.Header{"Origin": []string{"https://attacker.example"}, "Authorization": []string{"Bearer test-device-token-0123456789abcdef0123456789"}}
	conn, resp, err := websocket.DefaultDialer.Dial(websocketURL(server.URL), header)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("cross-origin WebSocket upgrade unexpectedly succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("upgrade status = %v, want 403", resp)
	}
}

func TestProducerLoggingDoesNotExposeDeviceOrLocation(t *testing.T) {
	logger := logrus.StandardLogger()
	previousOutput := logger.Out
	previousFormatter := logger.Formatter
	previousLevel := logger.Level
	var output bytes.Buffer
	logger.SetOutput(&output)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	t.Cleanup(func() {
		logger.SetOutput(previousOutput)
		logger.SetFormatter(previousFormatter)
		logger.SetLevel(previousLevel)
	})

	producer := newRecordingProducer()
	loggerMiddleware := NewLogMiddleware(producer)
	if err := loggerMiddleware.ProduceData(types.OBUData{TenantID: types.LegacyTenantID, OBUID: 7654321, Latitude: 23.7806, Longitude: 90.4074}); err != nil {
		t.Fatal(err)
	}
	logged := output.String()
	for _, sensitive := range []string{"7654321", "23.7806", "90.4074", "obuID", "latitude", "longitude"} {
		if strings.Contains(logged, sensitive) {
			t.Errorf("log contains sensitive value %q: %s", sensitive, logged)
		}
	}
}

func dialWebSocket(t *testing.T, serverURL string, header http.Header) *websocket.Conn {
	t.Helper()
	if header == nil {
		header = http.Header{"Authorization": []string{"Bearer test-device-token-0123456789abcdef0123456789"}}
	}
	conn, resp, err := websocket.DefaultDialer.Dial(websocketURL(serverURL), header)
	if err != nil {
		if resp != nil {
			t.Fatalf("WebSocket dial failed with status %d: %v", resp.StatusCode, err)
		}
		t.Fatalf("WebSocket dial failed: %v", err)
	}
	return conn
}

func websocketURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + "/ws"
}
