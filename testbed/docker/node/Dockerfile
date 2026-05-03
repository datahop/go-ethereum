FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev linux-headers git

COPY go.mod go.sum /go-ethereum/
RUN cd /go-ethereum && go mod download

COPY . /go-ethereum/
RUN cd /go-ethereum && go build -o /devp2p ./cmd/devp2p

FROM alpine:3.20

RUN apk add --no-cache iproute2 bash
COPY --from=builder /devp2p /usr/local/bin/devp2p
COPY testbed/docker/node/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
