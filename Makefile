a1:
	go run main.go analyze authors20231218 -p 1 -t 40
a2:
	go run main.go analyze authors20231218 -p 2 -t 40
a3:
	go run main.go analyze authors20231218 -p 3 -t 10
o1:
	go run main.go  loadToCsv  -p 7 -v 20231225  -O /mnt/sata3/openalex/parse_output -t 40