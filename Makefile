
clean:
	go clean -i ./...
	rm -rf ./tmp/*

test:
	go test -cover ./...
