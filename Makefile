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
# 		case 4:
# 			cp := load.NewFunderProject(foldPath)
# 			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
# 		case 5:
# 			cp := load.NewSourceProject(foldPath)
# 			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
# 		case 6:
# 			cp := load.NewAuthorProject(foldPath)
# 			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
# 		case 7:
# 			cp := load.NewWorkProject(foldPath)
# 			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
# 		case 8:
# 			cp := load.NewTopicProject(foldPath)
# 			load.RuntimeToNDJSONFlow(cp, treadCount, Version, outPath, outFileCount)
# 	go run main.go  loadToNDJSON -v 20251210  -O /mnt/hg01/openalex/parse_output -t 40  -p 7  -c 10
# # Concept
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 1  
## # Institution
# 	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 2 
# # Publisher
	go run main.go  loadToNDJSON -v 20251210  -O  /mnt/hg01/openalex/parse_output -t 20 -c 1 -p 3

o2:
	go run main.go  loadToNDJSON -v 20241201  -O  /mnt/hg01/openalex/parse_output -t 40  -p 1  -c 1
	go run main.go  loadToNDJSON -v 20241201  -O  /mnt/hg01/openalex/parse_output -t 40  -p 6  -c 1

loadToMongo:
	go run main.go loadToMongo