package main

import (
	"strconv"
	"sync"
	"strings"
	"container/heap"
	"time"
	"errors"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"DEL":     del,
	"HDEL":    hdel,
	"EXPIRE":  expire,
}

var expirehandlers = map[string]func(string, time.Time) error{
	"nx": setexpirenx,
	"xx": setexpirexx,
	"gt": setexpiregt,
	"lt": setexpirelt,
}

var SETs = map[string]string{}

// sets mutex lock
var SETsMu = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

var PQ = &PriorityQueue{}
var HEAPMu = sync.RWMutex{}


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
		return Value{typ: "error", str: "ERRR: Wrong number of args for HSET"}
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

func del(args []Value) Value {
	if len(args) < 1 {
		return Value{typ: "error", str: "ERR: Wrong number of args for DEL"}
	}

	key := args[0].bulk

	//Lock Set while Mutating
	SETsMu.Lock()
	delete(SETs, key)
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}

}

func hdel(args []Value) Value {

	if len(args) < 2 {
		return Value{typ: "error", str: "ERRR: Wrong number of args for HDEL"}
	}

	key := args[0].bulk
	key2 := args[1].bulk

	HSETsMu.Lock()
	delete(HSETs[key], key2)
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}

}

/*
Steps for Expire:
[x] Create data strcut for commands?
[x] Allow support for expire time on data struct
[x] Complete Handler
[x]  Complete option heleprs
[] Repear to clear out expired data
[] handle AOF somehow

EXPIRE key seconds [NX | XX | GT | LT]:

The EXPIRE command supports a set of options:

NX -- Set expiry only when the key has no expiry
XX -- Set expiry only when the key has an existing expiry
GT -- Set expiry only when the new expiry is greater than current one
LT -- Set expiry only when the new expiry is less than current one

How to handle data struct (ideas):

 1. a min heap with (time, key)
    the lowest amount of time left should always be on top
    reaper checks the top amount, if < time, delete and pop, check next val
    and continue
*/
func expire(args []Value) Value {

	if len(args) != 3 {
		return Value{typ: "error", str: "ERR: Wrong number of args for EXPIRE"}
	}

	key := args[0].bulk
	secString := args[1].bulk

  secInt, err := strconv.Atoi(secString)
  if err != nil {
  	return Value{typ: "error", str: "ERR: seconds needs to be a number"}
	}
	
	// Now calculate the absolute time
	seconds := time.Now().Add(time.Duration(secInt) * time.Second)
	
	if err != nil {
		return Value{typ: "error", str: "ERR: seconds needs to be a number"}
	}

	option := args[2].bulk

//	fmt.Println(key, seconds, option)

	ehandler, ok := expirehandlers[strings.ToLower(option)]

	if !ok {
		return Value{typ: "error", str: "ERR: option provided is not a valid option."}
	}

	err = ehandler(key, seconds)

	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	return Value{typ: "bulk", bulk: "Expire Time Set"}

}

func setexpirenx(key string, seconds time.Time) error {
	
	HEAPMu.Lock()
	defer HEAPMu.Unlock()

	pqitem, _ := PQ.Exists(key)

	if pqitem == nil {
		heap.Push(PQ, &PQItem{Priority: seconds, Value: key})
	} 
	return nil
}

func setexpirexx(key string, seconds time.Time) error {
	
	HEAPMu.Lock()
	defer HEAPMu.Unlock()

	pqitem, _ := PQ.Exists(key)

	if pqitem != nil {	
		heap.Push(PQ, &PQItem{Priority: seconds, Value: key})
	}

	return nil
}

func setexpiregt(key string, seconds time.Time) error {

	HEAPMu.Lock()
	defer HEAPMu.Unlock()

	pqitem, i := PQ.Exists(key)

	if pqitem == nil {
		return errors.New("Value does not have an expiration.")
	}
	
  if pqitem.Priority.After(seconds) {
		return errors.New("The expiration time is not greater than current expiration time")
  }

  pqitem.Priority = seconds
  heap.Fix(PQ, i)

	return nil
}

func setexpirelt(key string, seconds time.Time) error {
	
	HEAPMu.Lock()
	defer HEAPMu.Unlock()
	
	pqitem, i := PQ.Exists(key)

	if pqitem == nil {
		return errors.New("Value does not have an expiration.")
	}

	if pqitem.Priority.Before(seconds) {
		return errors.New("The expiration time is greater than current expiration time.")
	}

	pqitem.Priority = seconds
	heap.Fix(PQ, i)

	return nil
}
