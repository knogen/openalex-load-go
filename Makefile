SHELL := /bin/bash

ifneq (,$(wildcard ./.env))
    include .env
    export
endif


a1:
	go run main.go analyze authors20231218 -p 1 -t 40
a2:
	go run main.go analyze authors20231218 -p 2 -t 40
a3:
	go run main.go analyze authors20231218 -p 3 -t 10
o1:
	# go run main.go  loadToCsv -v 20241031  -O /mnt/sata3/openalex/parse_output -t 40  -p 7 
	go run main.go  loadToCsv -v 20241201  -O  /mnt/sata3/openalex/parse_output -t 1  -p 8  -c 1

o2:
	go run main.go  loadToCsv -v 20241201  -O  /mnt/sata3/openalex/parse_output -t 40  -p 1  -c 1
	go run main.go  loadToCsv -v 20241201  -O  /mnt/sata3/openalex/parse_output -t 40  -p 6  -c 1

loadToMongo:
	go run main.go loadToMongo