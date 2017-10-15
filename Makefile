
clean:
	go clean -i ./...
	rm -rf ./tmp/*

deps:
	go get -d ./...
	
test:
	go test -cover $(shell go list ./... | grep -v /vendor/)
