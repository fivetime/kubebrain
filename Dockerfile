ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
ARG BUILDPLATFORM=linux/amd64

FROM --platform=${BUILDPLATFORM} golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS build

WORKDIR /src
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
COPY third_party/tikv-client-go/go.mod third_party/tikv-client-go/go.sum ./third_party/tikv-client-go/
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
    && go build -trimpath -o /src/bin/kubebrain-operation-archive-verifier ./hack/production/cmd/operation-archive-verifier \
    && go build -trimpath -o /src/bin/kubebrain-operation-audit ./hack/production/cmd/operation-audit \
    && go build -trimpath -o /src/bin/kubebrain-scan-memory-probe ./hack/production/cmd/scan-memory-probe \
    && go build -trimpath -o /src/bin/kubebrain-tikv-repair-alert-receiver ./hack/production/cmd/tikv-repair-alert-receiver \
    && go build -trimpath -o /src/bin/kubebrain-rollout-availability-probe ./hack/production/cmd/rollout-availability-probe \
    && go build -trimpath -o /src/bin/kubebrain-jwt-token-probe ./hack/production/cmd/jwt-token-probe \
    && go build -trimpath -o /src/bin/kubebrain-jwt-token-issuer ./hack/production/cmd/jwt-token-issuer \
    && go build -trimpath -o /src/bin/kubebrain-jwt-kms-export-verifier ./hack/production/cmd/jwt-kms-export-verifier \
    && go build -trimpath -o /src/bin/kubebrain-jwt-kms-export-client ./hack/production/cmd/jwt-kms-export-client \
    && go build -trimpath -o /src/bin/kubebrain-jwt-kms-lifecycle-verifier ./hack/production/cmd/jwt-kms-lifecycle-verifier \
    && go build -trimpath -o /src/bin/kubebrain-jwt-kms-lifecycle-client ./hack/production/cmd/jwt-kms-lifecycle-client \
    && go build -trimpath -o /src/bin/kubebrain-jwt-rotation-publisher ./hack/production/cmd/jwt-rotation-publisher \
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
    && go build -trimpath -o /src/bin/kubebrain-metering-payment-ledger ./hack/production/cmd/metering-payment-ledger \
    && go build -trimpath -o /src/bin/kubebrain-metering-ledger-export ./hack/production/cmd/metering-ledger-export \
    && go build -trimpath -o /src/bin/kubebrain-metering-invoice-number ./hack/production/cmd/metering-invoice-number \
    && go build -trimpath -o /src/bin/kubebrain-logical-export ./hack/backup/cmd/logical-export \
    && go build -trimpath -o /src/bin/kubebrain-logical-status ./hack/backup/cmd/logical-status \
    && go build -trimpath -o /src/bin/kubebrain-cold-snapshot-receipt ./hack/backup/cmd/cold-snapshot-receipt \
    && go build -trimpath -o /src/bin/kubebrain-cold-restore-render ./hack/backup/cmd/cold-restore-render \
    && go build -trimpath -o /src/bin/kubebrain-legacy-snapshot-remediation ./hack/backup/cmd/legacy-snapshot-remediation \
    && go build -trimpath -o /src/bin/kubebrain-storage-capacity-verify ./hack/backup/cmd/storage-capacity-verify \
    && go build -trimpath -o /src/bin/kubebrain-logical-verify ./hack/backup/cmd/logical-verify \
    && go build -trimpath -o /src/bin/kubebrain-logical-etcd-snapshot ./hack/backup/cmd/logical-etcd-snapshot \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-full-backup ./hack/backup/cmd/native-pitr-full-backup \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-full-restore ./hack/backup/cmd/native-pitr-full-restore \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-admission-fence ./hack/backup/cmd/native-pitr-admission-fence \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-full-artifact-verify ./hack/backup/cmd/native-pitr-full-artifact-verify \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-full-restore-receipt-verify ./hack/backup/cmd/native-pitr-full-restore-receipt-verify \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-full-snapshot-receipt ./hack/backup/cmd/native-pitr-full-snapshot-receipt \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-log-artifact-verify ./hack/backup/cmd/native-pitr-log-artifact-verify \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-log-replay ./hack/backup/cmd/native-pitr-log-replay \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-preflight ./hack/backup/cmd/native-pitr-preflight \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-restoration-fence ./hack/backup/cmd/native-pitr-restoration-fence \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-restore-plan ./hack/backup/cmd/native-pitr-restore-plan \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-semantic-verify ./hack/backup/cmd/native-pitr-semantic-verify \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-source-capture ./hack/backup/cmd/native-pitr-source-capture \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-source-exclusive ./hack/backup/cmd/native-pitr-source-exclusive \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-replacement-handoff ./hack/backup/cmd/native-pitr-target-replacement-handoff \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-provisioning-receipt ./hack/backup/cmd/native-pitr-target-provisioning-receipt \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-retirement-receipt ./hack/backup/cmd/native-pitr-target-retirement-receipt \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-retirement-authorize ./hack/backup/cmd/native-pitr-target-retirement-authorize \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-provision-control ./hack/backup/cmd/native-pitr-target-provision-control \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-target-empty ./hack/backup/cmd/native-pitr-target-empty \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-task-create ./hack/backup/cmd/native-pitr-task-create \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-task-delete ./hack/backup/cmd/native-pitr-task-delete \
    && go build -trimpath -o /src/bin/kubebrain-native-pitr-task-ready ./hack/backup/cmd/native-pitr-task-ready \
    && go build -trimpath -o /src/bin/kubebrain-etcd-audit-probe ./hack/production/cmd/etcd-audit-probe \
    && go build -trimpath -o /src/bin/kubebrain-uid-delete ./hack/production/cmd/uid-delete \
    && cd /src/hack/backup/objectstore \
    && go build -trimpath -o /src/bin/kubebrain-logical-object ./cmd/logical-object

FROM pingcap/br:v7.5.1@sha256:7815531bc337a56845efc3dceb9fe75778d387d76927994df736b6e23a98f768 AS br-v751

# Build this target explicitly for the native PITR full-backup Job. Keeping BR
# out of the default data-plane image avoids adding its ~218 MiB toolchain to
# every KubeBrain/operation pod while still pinning both supported arches to
# the exact upstream manifest list audited by the receipt contract.
FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS native-pitr-full-backup

RUN apk add --no-cache \
      bash=5.3.3-r1 \
      ca-certificates=20260611-r0 \
      coreutils=9.8-r1 \
      gcompat=1.1.0-r4 \
      jq=1.8.1-r0 \
    && addgroup -S -g 65532 kubebrain \
    && adduser -S -D -H -h /nonexistent -s /sbin/nologin -u 65532 -G kubebrain kubebrain

COPY --from=build /src/bin/kubebrain-native-pitr-full-backup /usr/local/bin/kubebrain-native-pitr-full-backup
COPY --from=build /src/bin/kubebrain-operation-worker /usr/local/bin/kubebrain-operation-worker
COPY --from=build /src/bin/kubebrain-operationctl /usr/local/bin/kubebrain-operationctl
COPY --from=br-v751 /br /usr/local/bin/br
COPY hack/production/run-native-pitr-full-backup-operation.sh hack/production/operation-time-validation.sh /opt/kubebrain/hack/production/

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
LABEL org.opencontainers.image.title="KubeBrain native PITR full backup" \
      org.opencontainers.image.version="$KUBEBRAIN_VERSION" \
      org.opencontainers.image.revision="$KUBEBRAIN_GIT_SHA" \
      org.opencontainers.image.created="$KUBEBRAIN_BUILD_DATE"

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kubebrain-native-pitr-full-backup"]

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS native-pitr-full-restore

RUN apk add --no-cache \
      bash=5.3.3-r1 \
      ca-certificates=20260611-r0 \
      coreutils=9.8-r1 \
      gcompat=1.1.0-r4 \
      jq=1.8.1-r0 \
    && addgroup -S -g 65532 kubebrain \
    && adduser -S -D -H -h /nonexistent -s /sbin/nologin -u 65532 -G kubebrain kubebrain

COPY --from=build /src/bin/kubebrain-native-pitr-full-restore /usr/local/bin/kubebrain-native-pitr-full-restore
COPY --from=build /src/bin/kubebrain-native-pitr-full-restore-receipt-verify /usr/local/bin/kubebrain-native-pitr-full-restore-receipt-verify
COPY --from=build /src/bin/kubebrain-native-pitr-target-replacement-handoff /usr/local/bin/kubebrain-native-pitr-target-replacement-handoff
COPY --from=build /src/bin/kubebrain-native-pitr-target-provisioning-receipt /usr/local/bin/kubebrain-native-pitr-target-provisioning-receipt
COPY --from=build /src/bin/kubebrain-native-pitr-target-retirement-receipt /usr/local/bin/kubebrain-native-pitr-target-retirement-receipt
COPY --from=build /src/bin/kubebrain-native-pitr-target-retirement-authorize /usr/local/bin/kubebrain-native-pitr-target-retirement-authorize
COPY --from=build /src/bin/kubebrain-native-pitr-target-provision-control /usr/local/bin/kubebrain-native-pitr-target-provision-control
COPY --from=build /src/bin/kubebrain-native-pitr-target-empty /usr/local/bin/kubebrain-native-pitr-target-empty
COPY --from=build /src/bin/kubebrain-operation-worker /usr/local/bin/kubebrain-operation-worker
COPY --from=build /src/bin/kubebrain-operationctl /usr/local/bin/kubebrain-operationctl
COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl
COPY --from=br-v751 /br /usr/local/bin/br
COPY hack/production/run-native-pitr-full-restore-operation.sh hack/production/operation-time-validation.sh /opt/kubebrain/hack/production/

ARG KUBEBRAIN_VERSION
ARG KUBEBRAIN_GIT_SHA
ARG KUBEBRAIN_BUILD_DATE
LABEL org.opencontainers.image.title="KubeBrain native PITR full restore" \
      org.opencontainers.image.version="$KUBEBRAIN_VERSION" \
      org.opencontainers.image.revision="$KUBEBRAIN_GIT_SHA" \
      org.opencontainers.image.created="$KUBEBRAIN_BUILD_DATE"

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kubebrain-native-pitr-full-restore"]

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
COPY --from=build /src/bin/kubebrain-operation-archive-verifier /usr/local/bin/kubebrain-operation-archive-verifier
COPY --from=build /src/bin/kubebrain-operation-audit /usr/local/bin/kubebrain-operation-audit
COPY --from=build /src/bin/kubebrain-scan-memory-probe /usr/local/bin/kubebrain-scan-memory-probe
COPY --from=build /src/bin/kubebrain-tikv-repair-alert-receiver /usr/local/bin/kubebrain-tikv-repair-alert-receiver
COPY --from=build /src/bin/kubebrain-rollout-availability-probe /usr/local/bin/kubebrain-rollout-availability-probe
COPY --from=build /src/bin/kubebrain-jwt-token-probe /usr/local/bin/kubebrain-jwt-token-probe
COPY --from=build /src/bin/kubebrain-jwt-token-issuer /usr/local/bin/kubebrain-jwt-token-issuer
COPY --from=build /src/bin/kubebrain-jwt-kms-export-verifier /usr/local/bin/kubebrain-jwt-kms-export-verifier
COPY --from=build /src/bin/kubebrain-jwt-kms-export-client /usr/local/bin/kubebrain-jwt-kms-export-client
COPY --from=build /src/bin/kubebrain-jwt-kms-lifecycle-verifier /usr/local/bin/kubebrain-jwt-kms-lifecycle-verifier
COPY --from=build /src/bin/kubebrain-jwt-kms-lifecycle-client /usr/local/bin/kubebrain-jwt-kms-lifecycle-client
COPY --from=build /src/bin/kubebrain-jwt-rotation-publisher /usr/local/bin/kubebrain-jwt-rotation-publisher
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
COPY --from=build /src/bin/kubebrain-metering-payment-ledger /usr/local/bin/kubebrain-metering-payment-ledger
COPY --from=build /src/bin/kubebrain-metering-ledger-export /usr/local/bin/kubebrain-metering-ledger-export
COPY --from=build /src/bin/kubebrain-metering-invoice-number /usr/local/bin/kubebrain-metering-invoice-number
COPY --from=build /src/bin/kubebrain-logical-object /usr/local/bin/kubebrain-logical-object
COPY --from=build /src/bin/kubebrain-logical-export /usr/local/bin/kubebrain-logical-export
COPY --from=build /src/bin/kubebrain-logical-status /usr/local/bin/kubebrain-logical-status
COPY --from=build /src/bin/kubebrain-cold-snapshot-receipt /usr/local/bin/kubebrain-cold-snapshot-receipt
COPY --from=build /src/bin/kubebrain-cold-restore-render /usr/local/bin/kubebrain-cold-restore-render
COPY --from=build /src/bin/kubebrain-legacy-snapshot-remediation /usr/local/bin/kubebrain-legacy-snapshot-remediation
COPY --from=build /src/bin/kubebrain-native-pitr-admission-fence /usr/local/bin/kubebrain-native-pitr-admission-fence
COPY --from=build /src/bin/kubebrain-native-pitr-full-artifact-verify /usr/local/bin/kubebrain-native-pitr-full-artifact-verify
COPY --from=build /src/bin/kubebrain-native-pitr-full-snapshot-receipt /usr/local/bin/kubebrain-native-pitr-full-snapshot-receipt
COPY --from=build /src/bin/kubebrain-native-pitr-log-artifact-verify /usr/local/bin/kubebrain-native-pitr-log-artifact-verify
COPY --from=build /src/bin/kubebrain-native-pitr-log-replay /usr/local/bin/kubebrain-native-pitr-log-replay
COPY --from=build /src/bin/kubebrain-native-pitr-preflight /usr/local/bin/kubebrain-native-pitr-preflight
COPY --from=build /src/bin/kubebrain-native-pitr-restoration-fence /usr/local/bin/kubebrain-native-pitr-restoration-fence
COPY --from=build /src/bin/kubebrain-native-pitr-restore-plan /usr/local/bin/kubebrain-native-pitr-restore-plan
COPY --from=build /src/bin/kubebrain-native-pitr-semantic-verify /usr/local/bin/kubebrain-native-pitr-semantic-verify
COPY --from=build /src/bin/kubebrain-native-pitr-source-capture /usr/local/bin/kubebrain-native-pitr-source-capture
COPY --from=build /src/bin/kubebrain-native-pitr-source-exclusive /usr/local/bin/kubebrain-native-pitr-source-exclusive
COPY --from=build /src/bin/kubebrain-native-pitr-target-replacement-handoff /usr/local/bin/kubebrain-native-pitr-target-replacement-handoff
COPY --from=build /src/bin/kubebrain-native-pitr-target-provisioning-receipt /usr/local/bin/kubebrain-native-pitr-target-provisioning-receipt
COPY --from=build /src/bin/kubebrain-native-pitr-target-retirement-receipt /usr/local/bin/kubebrain-native-pitr-target-retirement-receipt
COPY --from=build /src/bin/kubebrain-native-pitr-target-retirement-authorize /usr/local/bin/kubebrain-native-pitr-target-retirement-authorize
COPY --from=build /src/bin/kubebrain-native-pitr-target-provision-control /usr/local/bin/kubebrain-native-pitr-target-provision-control
COPY --from=build /src/bin/kubebrain-native-pitr-target-empty /usr/local/bin/kubebrain-native-pitr-target-empty
COPY --from=build /src/bin/kubebrain-native-pitr-task-create /usr/local/bin/kubebrain-native-pitr-task-create
COPY --from=build /src/bin/kubebrain-native-pitr-task-delete /usr/local/bin/kubebrain-native-pitr-task-delete
COPY --from=build /src/bin/kubebrain-native-pitr-task-ready /usr/local/bin/kubebrain-native-pitr-task-ready
COPY --from=build /src/bin/kubebrain-storage-capacity-verify /usr/local/bin/kubebrain-storage-capacity-verify
COPY --from=build /src/bin/kubebrain-logical-verify /usr/local/bin/kubebrain-logical-verify
COPY --from=build /src/bin/kubebrain-logical-etcd-snapshot /usr/local/bin/kubebrain-logical-etcd-snapshot
COPY --from=build /src/bin/kubebrain-etcd-audit-probe /usr/local/bin/kubebrain-etcd-audit-probe
COPY --from=build /src/bin/kubebrain-uid-delete /usr/local/bin/kubebrain-uid-delete
COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl
COPY hack/backup/logical-export.sh hack/backup/logical-status.sh hack/backup/logical-verify.sh hack/backup/cold-snapshot-preflight.sh hack/backup/cold-snapshot-execute.sh hack/backup/cold-restore-execute.sh /opt/kubebrain/hack/backup/
COPY hack/production/*.sh /opt/kubebrain/hack/production/

RUN REQUIRE_INFO_EXECUTOR_BINARIES=true \
    /opt/kubebrain/hack/production/validate-info-executor-runtime.sh

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
