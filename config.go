package main

import (
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/joho/godotenv"
)

var (
	ConfigHttpListen   string
	ConfigSearcherList []string
)

func initConfig() {
	err := godotenv.Load()
	if err != nil {
		logrus.Fatal("failed to loading .env file")
	}
	ConfigHttpListen = os.Getenv("HTTP_LISTEN")
	if ConfigHttpListen == "" {
		logrus.Fatal("HTTP_LISTEN can not be blank")
	}
	originSearcherList := os.Getenv("SEARCHER_LIST")
	if originSearcherList == "" {
		logrus.Fatal("SEARCHER_LIST can not be blank")
	}
	ConfigSearcherList = strings.Split(originSearcherList, ",")
}
