package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var SETs = map[string]string{}

// sets mutex lock
var SETsMu = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func ping(args []Value) Value {

	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func set(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "ERR: Wrong number of args for SET"}
	}

	key := args[0].bulk
	value := args[1].bulk

	//Lock Set while Mutating
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR: Wrong number of args for GET"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	val, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

func hset(args []Value) Value {

	if len(args) != 3 {
		return Value{typ: "error", str: "ERRR: Wrong number of args for SET"}
	}

	key := args[0].bulk
	key2 := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[key]; !ok {
		HSETs[key] = map[string]string{}
	}
	HSETs[key][key2] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR: Wrong number of args for SET"}
	}

	key := args[0].bulk
	key2 := args[1].bulk

	HSETsMu.RLock()
	val, ok := HSETs[key][key2]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: val}
}

/*

hset {
	users {
		u1: colby,
		u2: colby
	},
	posts {
		p1: post,
		p2: post
	}
}

*/

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR: Wrong number of args for GET"}
	}

	hash := args[0].bulk
	returnStr := hash + "\n"

	HSETsMu.RLock()
	keys, ok := HSETs[hash]
	HSETsMu.RUnlock()

	for _, v := range keys {
		returnStr += string("- " + v + "\n")
	}

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: returnStr}
}
