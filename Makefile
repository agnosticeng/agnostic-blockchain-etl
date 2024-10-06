all: test build

build: 
	go build -o bin/agnostic-blockchain-etl ./cmd

test:
	go test -v ./...

clean:
	rm -rf bin
