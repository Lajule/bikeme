FROM golang:1.16.3-alpine AS builder
ARG version="development"
RUN apk add --no-cache git gcc libc-dev
WORKDIR /src
COPY . .
RUN go build -ldflags="-X 'main.Version=${version}'"

FROM alpine
WORKDIR /app
COPY --from=builder /src/bikeme .
ENTRYPOINT ["./bikeme"]
