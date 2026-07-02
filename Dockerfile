FROM golang:1.22-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG STORAGE=tikv
RUN case "$STORAGE" in \
      tikv) bash ./build/build-tikv.sh ;; \
      badger) bash ./build/build-badger.sh ;; \
      *) echo "unsupported STORAGE=$STORAGE" >&2; exit 1 ;; \
    esac

FROM debian:bookworm-slim

RUN useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin kubebrain

COPY --from=build /src/bin/kube-brain /usr/local/bin/kube-brain

USER 65532:65532
EXPOSE 3379 3380 8080

ENTRYPOINT ["/usr/local/bin/kube-brain"]
