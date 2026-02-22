FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/ingestion-worker ./cmd/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ingestion-worker /usr/local/bin/ingestion-worker

EXPOSE 9090

ENTRYPOINT ["ingestion-worker"]
