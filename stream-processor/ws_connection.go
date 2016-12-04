// This creates an endpoint for a websocket conncetion for a client.
// Periodically this will fetch stream data from kafka and broadcast it to connected clients.
//
package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func createWSConnection(w http.ResponseWriter, req *http.Request) {
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Println("Error upgrading WS connection", err)
		return
	}
	log.Println("Send message")
	conn.WriteMessage(websocket.TextMessage, []byte("Hi there"))
}

func main() {
	http.HandleFunc("/", createWSConnection)
	http.ListenAndServe(":8082", nil)
}
