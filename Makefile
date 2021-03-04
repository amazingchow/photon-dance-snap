test:
	go test -count=1 -v -p 1 $(shell go list ./... | grep -v snappb)