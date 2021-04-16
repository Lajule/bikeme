FROM golang:1.16.3-alpine AS builder
ARG VERSION="development"
RUN apk add --no-cache git patch gcc libc-dev make
WORKDIR /src
COPY . .
RUN go build -ldflags="-X 'main.Version=${VERSION}'"

FROM alpine
WORKDIR /
COPY --from=builder /src/bikeme .
ENTRYPOINT ["./bikeme"]
