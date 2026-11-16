package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const wsEndpoint = "ws://127.0.0.1:30000/ws"

var sendInterval = 5 * time.Second

type telemetryFrame struct {
	OBUID int     `json:"obuID"`
	Lat   float64 `json:"lat"`
	Long  float64 `json:"long"`
}

func genLatLong() (float64, float64) {
	return rand.Float64()*170 - 85, rand.Float64()*360 - 180
}

func main() {
	deviceToken := strings.TrimSpace(os.Getenv("FLEETTOLL_SIMULATOR_DEVICE_TOKEN"))
	if deviceToken == "" {
		log.Fatal("FLEETTOLL_SIMULATOR_DEVICE_TOKEN must be configured")
	}
	obuValue := strings.TrimSpace(os.Getenv("FLEETTOLL_SIMULATOR_OBU_ID"))
	if obuValue == "" {
		obuValue = "12345"
	}
	obuID, err := strconv.Atoi(obuValue)
	if err != nil || obuID <= 0 {
		log.Fatal("FLEETTOLL_SIMULATOR_OBU_ID must be a positive integer")
	}

	header := http.Header{"Authorization": []string{"Bearer " + deviceToken}}
	conn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, header)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	for {
		lat, long := genLatLong()
		frame := telemetryFrame{OBUID: obuID, Lat: lat, Long: long}
		fmt.Printf("Sending telemetry for OBU %d\n", obuID)
		if err := conn.WriteJSON(frame); err != nil {
			log.Fatal(err)
		}
		time.Sleep(sendInterval)
	}
}

func init() { rand.Seed(time.Now().UnixNano()) }
