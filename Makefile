.PHONY: build test race short vet cover fuzz bench soak e2e run clean
build:
	go build -o bin/ ./cmd/...
test:
	go test -count=1 ./...
race:                      ## everything, under the race detector (includes e2e)
	go test -race -count=1 ./...
short:                     ## fast loop: skips the e2e binaries and shortens model tests
	go test -short -race -count=1 ./...
e2e:                       ## build and drive the real binaries
	go test -count=1 -v ./e2e
vet:
	go vet ./... && test -z "$$(gofmt -l .)"
cover:
	go test -count=1 -coverpkg=./... -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1
fuzz:                      ## 20s smoke run of each fuzzer
	go test ./commitlog -run '^$$' -fuzz FuzzRecover -fuzztime 20s
	go test ./protocol -run '^$$' -fuzz FuzzParseMsgDifferential -fuzztime 20s
	go test ./protocol -run '^$$' -fuzz FuzzAppendMsgRoundTrip -fuzztime 20s
soak:                      ## long randomized run of the commit log model
	MODEL_SEEDS=2000 go test -count=1 -run TestModelRandomOps ./commitlog
bench:
	go test -run '^$$' -bench . -benchtime 2s ./commitlog ./server
run: build
	./bin/streamd -addr :7070 -data ./data
clean:
	rm -rf bin data cover.out
