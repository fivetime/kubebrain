module github.com/kubewharf/kubebrain/hack/etcd-client-compat

go 1.26

toolchain go1.26.4

require (
	github.com/anishathalye/porcupine v1.1.0
	github.com/coreos/go-semver v0.3.1
	github.com/stretchr/testify v1.11.1
	go.etcd.io/bbolt v1.5.0
	go.etcd.io/etcd/api/v3 v3.7.0-beta.0
	go.etcd.io/etcd/client/v3 v3.7.0-beta.0
	go.uber.org/zap v1.27.1
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.7.0-beta.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	go.etcd.io/etcd/api/v3 => /root/etcd/api
	go.etcd.io/etcd/client/pkg/v3 => /root/etcd/client/pkg
	go.etcd.io/etcd/client/v3 => /root/etcd/client/v3
)
