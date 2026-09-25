# Builds the persistent MySQL server. The module replaces Badger with the
# sibling directory, passed in as an extra build context named "badger".
FROM golang:1.26-bookworm AS build

RUN apt-get update \
	&& apt-get install -y --no-install-recommends g++ libicu-dev \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY --from=badger . /src/badger
COPY . /src/go-mysql-server

WORKDIR /src/go-mysql-server
RUN go build -o /out/persist ./_persist

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libicu72 \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /out/persist /usr/local/bin/persist

WORKDIR /data
ENV GMS_DATA=/data/gms
EXPOSE 3306 7001
ENTRYPOINT ["/usr/local/bin/persist"]
