ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
ARG BUILDPLATFORM=linux/amd64

FROM --platform=${BUILDPLATFORM} golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS build

WORKDIR /src
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN go mod download

COPY hack/backup/objectstore/go.mod hack/backup/objectstore/go.sum ./hack/backup/objectstore/
RUN cd hack/backup/objectstore && go mod download

ARG TARGETARCH=amd64
ENV GOOS=linux GOARCH=${TARGETARCH}
ARG KUBECTL_VERSION=v1.36.2
RUN mkdir -p /src/bin \
    && case "$TARGETARCH" in \
      amd64) KUBECTL_SHA256=1e9045ec32bea85da43de85f0065358529ea7c7a152eca78154fba5b58c27d82 ;; \
      arm64) KUBECTL_SHA256=c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl --proto '=https' --tlsv1.2 -fsSLo /src/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
    && echo "${KUBECTL_SHA256}  /src/bin/kubectl" | sha256sum -c - \
    && chmod 0755 /src/bin/kubectl

COPY . .

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
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
    && go build -trimpath -o /src/bin/kubebrain-operation-worker ./hack/production/cmd/operation-worker \
    && go build -trimpath -o /src/bin/kubebrain-operation-parameter-broker ./hack/production/cmd/operation-parameter-broker \
    && go build -trimpath -o /src/bin/kubebrain-operation-archiver ./hack/production/cmd/operation-archiver \
    && go build -trimpath -o /src/bin/kubebrain-operation-audit ./hack/production/cmd/operation-audit \
    && go build -trimpath -o /src/bin/kubebrain-metering-archive ./hack/production/cmd/metering-archive \
    && go build -trimpath -o /src/bin/kubebrain-metering-rollup ./hack/production/cmd/metering-rollup \
    && go build -trimpath -o /src/bin/kubebrain-metering-charge ./hack/production/cmd/metering-charge \
    && go build -trimpath -o /src/bin/kubebrain-metering-price-publish ./hack/production/cmd/metering-price-publish \
    && go build -trimpath -o /src/bin/kubebrain-metering-storage-archive ./hack/production/cmd/metering-storage-archive \
    && go build -trimpath -o /src/bin/kubebrain-metering-storage-rollup ./hack/production/cmd/metering-storage-rollup \
    && go build -trimpath -o /src/bin/kubebrain-metering-settlement-publish ./hack/production/cmd/metering-settlement-publish \
    && go build -trimpath -o /src/bin/kubebrain-metering-invoice-finalize ./hack/production/cmd/metering-invoice-finalize \
    && go build -trimpath -o /src/bin/kubebrain-metering-provider-statement ./hack/production/cmd/metering-provider-statement \
    && go build -trimpath -o /src/bin/kubebrain-metering-provider-reconcile ./hack/production/cmd/metering-provider-reconcile \
    && go build -trimpath -o /src/bin/kubebrain-logical-export ./hack/backup/cmd/logical-export \
    && go build -trimpath -o /src/bin/kubebrain-logical-status ./hack/backup/cmd/logical-status \
    && go build -trimpath -o /src/bin/kubebrain-logical-verify ./hack/backup/cmd/logical-verify \
    && go build -trimpath -o /src/bin/kubebrain-etcd-audit-probe ./hack/production/cmd/etcd-audit-probe \
    && go build -trimpath -o /src/bin/kubebrain-uid-delete ./hack/production/cmd/uid-delete \
    && cd /src/hack/backup/objectstore \
    && go build -trimpath -o /src/bin/kubebrain-logical-object ./cmd/logical-object

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

RUN apk add --no-cache \
      bash=5.3.3-r1 \
      ca-certificates=20260611-r0 \
      coreutils=9.8-r1 \
      curl=8.20.0-r0 \
      etcd-ctl=3.6.10-r1 \
      jq=1.8.1-r0 \
      openssl=3.5.7-r0 \
    && addgroup -S -g 65532 kubebrain \
    && adduser -S -D -H -h /nonexistent -s /sbin/nologin -u 65532 -G kubebrain kubebrain

COPY --from=build /src/bin/kube-brain /usr/local/bin/kube-brain
COPY --from=build /src/bin/kubebrain-backup-scheduler /usr/local/bin/kubebrain-backup-scheduler
COPY --from=build /src/bin/kubebrain-operation-api /usr/local/bin/kubebrain-operation-api
COPY --from=build /src/bin/kubebrain-operationctl /usr/local/bin/kubebrain-operationctl
COPY --from=build /src/bin/kubebrain-operation-worker /usr/local/bin/kubebrain-operation-worker
COPY --from=build /src/bin/kubebrain-operation-parameter-broker /usr/local/bin/kubebrain-operation-parameter-broker
COPY --from=build /src/bin/kubebrain-operation-archiver /usr/local/bin/kubebrain-operation-archiver
COPY --from=build /src/bin/kubebrain-operation-audit /usr/local/bin/kubebrain-operation-audit
COPY --from=build /src/bin/kubebrain-metering-archive /usr/local/bin/kubebrain-metering-archive
COPY --from=build /src/bin/kubebrain-metering-rollup /usr/local/bin/kubebrain-metering-rollup
COPY --from=build /src/bin/kubebrain-metering-charge /usr/local/bin/kubebrain-metering-charge
COPY --from=build /src/bin/kubebrain-metering-price-publish /usr/local/bin/kubebrain-metering-price-publish
COPY --from=build /src/bin/kubebrain-metering-storage-archive /usr/local/bin/kubebrain-metering-storage-archive
COPY --from=build /src/bin/kubebrain-metering-storage-rollup /usr/local/bin/kubebrain-metering-storage-rollup
COPY --from=build /src/bin/kubebrain-metering-settlement-publish /usr/local/bin/kubebrain-metering-settlement-publish
COPY --from=build /src/bin/kubebrain-metering-invoice-finalize /usr/local/bin/kubebrain-metering-invoice-finalize
COPY --from=build /src/bin/kubebrain-metering-provider-statement /usr/local/bin/kubebrain-metering-provider-statement
COPY --from=build /src/bin/kubebrain-metering-provider-reconcile /usr/local/bin/kubebrain-metering-provider-reconcile
COPY --from=build /src/bin/kubebrain-logical-object /usr/local/bin/kubebrain-logical-object
COPY --from=build /src/bin/kubebrain-logical-export /usr/local/bin/kubebrain-logical-export
COPY --from=build /src/bin/kubebrain-logical-status /usr/local/bin/kubebrain-logical-status
COPY --from=build /src/bin/kubebrain-logical-verify /usr/local/bin/kubebrain-logical-verify
COPY --from=build /src/bin/kubebrain-etcd-audit-probe /usr/local/bin/kubebrain-etcd-audit-probe
COPY --from=build /src/bin/kubebrain-uid-delete /usr/local/bin/kubebrain-uid-delete
COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl
COPY hack/backup/logical-export.sh hack/backup/logical-status.sh hack/backup/logical-verify.sh /opt/kubebrain/hack/backup/
COPY hack/production/*.sh /opt/kubebrain/hack/production/

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
LABEL org.opencontainers.image.title="KubeBrain" \
      org.opencontainers.image.version="$KUBEBRAIN_VERSION" \
      org.opencontainers.image.revision="$KUBEBRAIN_GIT_SHA" \
      org.opencontainers.image.created="$KUBEBRAIN_BUILD_DATE"

USER 65532:65532
EXPOSE 3379 3380 8080

ENTRYPOINT ["/usr/local/bin/kube-brain"]
