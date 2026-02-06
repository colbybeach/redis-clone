package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {

	HEAPMu.Lock()
 	heap.Init(PQ) 
  HEAPMu.Unlock()

	fmt.Println("Listening on port :6379")
	//Creating a passive socket
	//Colon means to listen on all available network interfaces like localhost, private ip, etc.
	l, err := net.Listen("tcp", ":6379")

	//Error syntax forces us to acknowledge errors are a part of everyhting and handle them
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	for {
		//This is blocking until something connects
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleConnection(conn, aof)

	}

	//Best pracitce to keep allocating and deallocating code near each other
	//Won't get caught returning without closing in the middle of the file if this line is here.
	// Infinite loop to recieve commands from clients and respond to them

}

func handleConnection(conn net.Conn, aof *Aof) {

	defer conn.Close()

	for {

		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(conn)

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" || command == "DEL" || command == "HDEL" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}

/*
Notes:
	What is net.listen -> net package, listenting on TCP port 6379
	what is defer -> postpone the execution of a function until the surrounding function returns
		- (common for cleanup tasks like closing files, unlocking mutexes, etc)
*/
