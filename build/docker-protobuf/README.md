# docker-protobuf

## Usage
```sh
docker run --rm -u $(shell id -u) -v$(PWD):$(PWD) -w$(PWD) ghcr.io/kakao/varlog-protobuf [OPTION] PROTO_FILES
```

## Build
```sh
export PAT=<Personal Access Token for Github Packages)
export USERNAME=<Github handle>
export TAG=$(cat TAG)
./build.sh
```

References:

- https://github.com/TheThingsIndustries/docker-protobuf
- https://github.com/jaegertracing/docker-protobuf
