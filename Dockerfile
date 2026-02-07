# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache build-base librdkafka-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -tags musl -o main .

# Final stage - using Alpine for smaller image
FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache ca-certificates librdkafka

COPY --from=builder /app/main .

ENTRYPOINT ["/app/main"]

