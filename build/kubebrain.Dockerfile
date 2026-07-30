# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.24

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS build

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH
ARG STORAGE=tikv
ARG VERSION=dev
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    case "${STORAGE}" in \
      tikv) storage_name=TiKV ;; \
      badger) storage_name=Badger ;; \
      *) echo "unsupported STORAGE=${STORAGE}" >&2; exit 1 ;; \
    esac; \
    CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH}" \
    go build -trimpath -tags "${STORAGE}" \
      -ldflags="-s -w \
        -X github.com/kubewharf/kubebrain/cmd/version.Version=${VERSION} \
        -X github.com/kubewharf/kubebrain/cmd/version.Storage=${storage_name} \
        -X github.com/kubewharf/kubebrain/cmd/version.GoVersion=$(go env GOVERSION) \
        -X github.com/kubewharf/kubebrain/cmd/version.GoOsArch=${TARGETOS:-linux}/${TARGETARCH} \
        -X github.com/kubewharf/kubebrain/cmd/version.GitSHA=${GIT_SHA} \
        -X github.com/kubewharf/kubebrain/cmd/version.Date=${BUILD_DATE}" \
      -o /out/kube-brain ./cmd

FROM alpine:${ALPINE_VERSION}

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 kubebrain \
    && adduser -S -D -H -h /nonexistent -s /sbin/nologin -u 65532 -G kubebrain kubebrain

COPY --from=build /out/kube-brain /usr/local/bin/kube-brain

USER 65532:65532
EXPOSE 3379 3380 8080

ENTRYPOINT ["/usr/local/bin/kube-brain"]
