package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

//Lets define constants for data types to make it easy to work with

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// define strcut to use in serilization and deserialization process
// easier to parse RESP commands
type Value struct {
	typ   string  //data type
	str   string  //holds value of string from simple strings
	num   int     // holds value of the int
	bulk  string  //store string from bulk strings
	array []Value // holds all the values from array

}

//create reader to contain all the methods to help use read
//from buffer and store in value

type Resp struct {
	reader *bufio.Reader
}

// Create a new Resp object
// Returns a pointer to this object
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// This creates a function attatched to the Resp struct
// lower case name means private
// named return parameters
func (r *Resp) readLine() (line []byte, n int, err error) {

	//read buffer one byte at a time until we reach the CRLF
	//append to line
	//return the line without last 2 bytes
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil

}

func (r *Resp) readInteger() (x int, n int, err error) {

	line, n, err := r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}

	//cast int because parseint returns 64-bit integer which is a
	//different datatype in go than normal int. Strongly typed problem
	return int(i64), n, nil

}

func (r *Resp) readArray() (Value, error) {

	v := Value{}
	v.typ = "array"

	//read length of the array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	//the Value array will be a array of values, size of length from before
	v.array = make([]Value, 0, length)

	for i := 0; i < length; i++ {
		val, err := r.Read()

		if err != nil {
			return v, err
		}

		v.array = append(v.array, val)
	}

	return v, nil

}

func (r *Resp) readBulk() (Value, error) {

	v := Value{}
	v.typ = "bulk"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, length)

	r.reader.Read(bulk)
	v.bulk = string(bulk)

	// Read trailing CRLF for bulk strings or else
	//pointer will be left at \r and not read next bulk correctly
	r.readLine()

	return v, nil

}

func (r *Resp) Read() (Value, error) {

	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// Marshal which will convert the Value to bytes
// representing the RESP response
func (v Value) Marshal() []byte {

	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

// Simply creating the string
// STRING = initial constant char
// v.str = actual string - ... means appending slice to slice
// \r\n = CRLF
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

//Itoa - int to ascii
/*
In Go, if you do byte(5), you get a non-printable character
(the 5th character in the ASCII table). You need the actual
text character '5' (which is ASCII code 53) so the client
can read the digits.
*/
func (v Value) marshalBulk() []byte {

	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalArray() []byte {

	len := len(v.array)
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		//recursively getting objects inside array
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}
