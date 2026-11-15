package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types" // Shared telemetry contract.
	"github.com/gorilla/websocket"
)

const wsEndpoint = "ws://127.0.0.1:30000/ws" // WebSocket server endpoint

var sendInterval = time.Second * 5 // Interval between sending data packets

// genLatLong generates a random latitude and longitude pair
func genLatLong() (float64, float64) {
	return genCoord(), genCoord()
}

// genCoord generates a random coordinate value
func genCoord() float64 {
	n := float64(rand.Intn(100) + 1) // Random integer part (1-100)
	f := rand.Float64()              // Random decimal part (0-1)
	return n + f                     // Combine to get the final coordinate
}

func main() {
	obuIDS := generateOBUIDS(20) // Generate 20 random OBU IDs

	// Establish a WebSocket connection
	conn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, nil)
	if err != nil {
		log.Fatal(err) // Terminate if connection fails
	}

	// Infinite loop to continuously send data to the server
	for {
		// Iterate over each OBU ID and send location data
		for i := 0; i < len(obuIDS); i++ {
			lat, long := genLatLong() // Generate random latitude and longitude
			data := types.OBUData{
				OBUID:     obuIDS[i],
				Latitude:  lat,
				Longitude: long,
			}

			// Print the data being sent
			fmt.Printf("Sending data: %+v\n", data)

			// Send the data as JSON over the WebSocket connection
			if err := conn.WriteJSON(data); err != nil {
				log.Fatal(err) // Terminate program if sending fails
			}
		}
		time.Sleep(sendInterval) // Wait before sending the next batch
	}
}

// generateOBUIDS generates 'n' random OBU IDs
func generateOBUIDS(n int) []int {
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		ids[i] = rand.Intn(999999) // Generate a random OBU ID (0-999999)
	}
	return ids
}

// init function initializes the random number generator seed
func init() {
	rand.Seed(time.Now().UnixNano()) // Ensure random values on each execution
}
