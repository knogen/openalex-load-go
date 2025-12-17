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
# # Concept
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 1  
# # Institution
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 2 
# # # Publisher
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 3
# # Funder
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 4
# # Source
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 5
# Author
	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 10 -p 6
# # Topic
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 8
# # Field
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 9
# # # Subfields
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 10

o2:
	go run main.go  loadToNDJSON -v 20251210  -O /mnt/hg01/openalex/parse_output -t 40 -p 7  -c 10

loadToMongo:
	go run main.go loadToMongo