#!/bin/bash
GOARCH=amd64 GOOS=linux go build -o sndmsg_amd64 ./demo.go
GOARCH=arm64 GOOS=linux go build -o sndmsg_arm ./demo.go
