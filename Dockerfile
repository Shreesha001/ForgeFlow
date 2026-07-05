FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /forgeflow ./cmd/forgeflow

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /forgeflow /usr/local/bin/forgeflow
EXPOSE 8080
ENTRYPOINT ["forgeflow"]
