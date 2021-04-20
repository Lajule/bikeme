# bikeme

Build your dream bike, A clustered REST API written Golang.

## Build

```sh
go build
```

## Docker image

```sh
docker build --build-arg version=1.0 -t bikeme:1.0 .
```

Run a container with following :

```sh
docker run -d --rm --name bikeme -v $PWD/config.json:/etc/config.json -p 8001:8001 bikeme:1.0 -c /etc/config.json
```
