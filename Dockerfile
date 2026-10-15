# syntax=docker/dockerfile:1
# A versão exata do Go deve acompanhar a diretiva `go` do go.mod (1.26.1).
FROM golang:1.26.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/broker-provision ./cmd/broker-provision \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sqs-producer ./cmd/sqs-producer

FROM alpine:3.22
RUN adduser -D -u 10001 app
COPY --from=build /out/api /out/migrate /out/broker-provision /out/sqs-producer /app/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/api"]
