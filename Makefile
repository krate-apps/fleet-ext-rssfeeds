.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/rssfeeds-worker ./cmd/rssfeeds-worker

pack:
	bash scripts/pack.sh dist
