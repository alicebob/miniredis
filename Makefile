.PHONY: all test testrace int

all: test

test:
	GO111MODULE=on go test ./...

testrace:
	GO111MODULE=on go test -race ./...

int:
	${MAKE} -C integration all
