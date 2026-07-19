ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE

FROM golang:1.26-bookworm AS build

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE

WORKDIR /src
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG STORAGE=tikv
RUN test -n "$KUBEBRAIN_VERSION" \
    && test -n "$KUBEBRAIN_GIT_SHA" \
    && test -n "$KUBEBRAIN_BUILD_DATE" \
    && export KUBEBRAIN_VERSION KUBEBRAIN_GIT_SHA KUBEBRAIN_BUILD_DATE \
    && export REQUIRE_BUILD_METADATA=true \
    && case "$STORAGE" in \
      tikv) bash ./build/build-tikv.sh ;; \
      badger) bash ./build/build-badger.sh ;; \
      *) echo "unsupported STORAGE=$STORAGE" >&2; exit 1 ;; \
    esac \
    && go build -trimpath -o /src/bin/kubebrain-backup-scheduler ./hack/production/cmd/backup-scheduler \
    && go build -trimpath -o /src/bin/kubebrain-operation-api ./hack/production/cmd/operation-api \
    && go build -trimpath -o /src/bin/kubebrain-operationctl ./hack/production/cmd/operationctl \
    && go build -trimpath -o /src/bin/kubebrain-operation-archiver ./hack/production/cmd/operation-archiver \
    && cd /src/hack/backup/objectstore \
    && go build -trimpath -o /src/bin/kubebrain-logical-object ./cmd/logical-object

FROM alpine:3.23

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE

LABEL org.opencontainers.image.title="KubeBrain" \
      org.opencontainers.image.version="$KUBEBRAIN_VERSION" \
      org.opencontainers.image.revision="$KUBEBRAIN_GIT_SHA" \
      org.opencontainers.image.created="$KUBEBRAIN_BUILD_DATE"

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 65532 kubebrain \
    && adduser -S -D -H -h /nonexistent -s /sbin/nologin -u 65532 -G kubebrain kubebrain

COPY --from=build /src/bin/kube-brain /usr/local/bin/kube-brain
COPY --from=build /src/bin/kubebrain-backup-scheduler /usr/local/bin/kubebrain-backup-scheduler
COPY --from=build /src/bin/kubebrain-operation-api /usr/local/bin/kubebrain-operation-api
COPY --from=build /src/bin/kubebrain-operationctl /usr/local/bin/kubebrain-operationctl
COPY --from=build /src/bin/kubebrain-operation-archiver /usr/local/bin/kubebrain-operation-archiver
COPY --from=build /src/bin/kubebrain-logical-object /usr/local/bin/kubebrain-logical-object

USER 65532:65532
EXPOSE 3379 3380 8080

ENTRYPOINT ["/usr/local/bin/kube-brain"]
