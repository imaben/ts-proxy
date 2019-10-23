package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

const (
	CodeSuccess         = 0
	CodeInvalidArgument = -1
	CodeInternalError   = -2
	CodeTimeout         = -3
	CodeExceedLimit     = -4
)

var (
	CodeMessageMapping map[int]string
)

type ResponseWarpper struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type RequestProperty struct {
	Content     string `json:"content"`
	SegmentWord bool   `json:"segment_word"`
}

type RequestIndex struct {
	ID         uint64                     `json:"id"`
	Properties map[string]RequestProperty `json:"properties"`
}

type RequestDelete struct {
	ID uint64 `json:"id"`
}

type RequestQuery struct {
	Offset     uint32
	Limit      uint32
	Properties map[string]RequestProperty
}

type ResponseDoc struct {
	ID         uint64                 `json:"id"`
	Properties map[string]interface{} `json:"properties"`
}

type ResponseQuery struct {
	Total uint32         `json:"total"`
	Docs  []*ResponseDoc `json:"docs"`
}

func response(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	js, err := json.Marshal(ResponseWarpper{Code: code, Message: CodeMessageMapping[code], Data: data})
	if err != nil {
		logrus.Fatal("json encoding fail:" + err.Error())
	}
	w.Write(js)
}

func httpHandlerIndex(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response(w, CodeInvalidArgument, nil)
		return
	}
	request := RequestIndex{}
	if err := json.Unmarshal(body, &request); err != nil {
		response(w, CodeInvalidArgument, nil)
		return
	}
	if request.ID == 0 || len(request.Properties) == 0 {
		response(w, CodeInvalidArgument, nil)
		return
	}
	code := searcher.Index(&request)
	response(w, code, nil)
}

func httpHandlerDelete(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response(w, CodeInvalidArgument, nil)
		return
	}
	request := RequestDelete{}
	if err := json.Unmarshal(body, &request); err != nil {
		response(w, CodeInvalidArgument, nil)
		return
	}
	code := searcher.Delete(&request)
	response(w, code, nil)
}

func httpHandlerQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	request := RequestQuery{}
	request.Properties = make(map[string]RequestProperty)
	for key, param := range r.URL.Query() {
		value := param[0]
		if value == "" {
			continue
		}
		if strings.ToLower(key) == "offset" {
			offset, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				response(w, CodeInvalidArgument, nil)
				return
			}
			request.Offset = uint32(offset)
			continue
		}
		if strings.ToLower(key) == "limit" {
			limit, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				response(w, CodeInvalidArgument, nil)
				return
			}
			request.Limit = uint32(limit)
			continue
		}
		var newkey string
		property := RequestProperty{}
		if strings.HasPrefix(key, "@") {
			property.SegmentWord = true
			newkey = key[1:]
		} else {
			property.SegmentWord = false
			newkey = key
		}
		property.Content = value
		fmt.Println(property)
		request.Properties[newkey] = property
	}
	code, resp := searcher.Query(&request)
	response(w, code, resp)
}

func initCodeMessageMapping() {
	CodeMessageMapping = map[int]string{
		CodeSuccess:         "ok",
		CodeInvalidArgument: "invalid argument",
		CodeInternalError:   "internal error",
		CodeTimeout:         "timeout",
		CodeExceedLimit:     "exceed the limit",
	}
}

func startHttpServer() {
	initCodeMessageMapping()
	router := httprouter.New()
	router.PUT("/", httpHandlerIndex)
	router.DELETE("/", httpHandlerDelete)
	router.GET("/", httpHandlerQuery)
	log.Fatal(http.ListenAndServe(ConfigHttpListen, router))
}
