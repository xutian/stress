#!/bin/bash
#GOARCH=amd64 GOOS=linux go build -o sndmsg ./demo.go
GOARCH=arm64 GOOS=linux go build -o sndmsg ./demo.go
