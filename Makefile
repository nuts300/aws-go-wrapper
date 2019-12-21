.PHONY: test

build:
	go build ./...

test:
	golint -set_exit_status . &
		golint -set_exit_status ./... &
		go vet ./... &
		go test -v ./...