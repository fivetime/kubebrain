module github.com/kubewharf/kubebrain

go 1.14

require (
	github.com/dgraph-io/badger v1.6.2
	github.com/evanphx/json-patch v0.5.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0
	github.com/google/btree v1.1.2
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/huandu/skiplist v1.1.0
	github.com/kubewharf/kubebrain-client v0.2.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.1
	github.com/tikv/client-go/v2 v2.0.7
	go.etcd.io/etcd/api/v3 v3.5.9
	go.etcd.io/etcd/client/v3 v3.5.9
	golang.org/x/sync v0.1.0
	google.golang.org/grpc v1.54.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.20.4 // indirect
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v0.20.2
	k8s.io/component-base v0.20.2
	k8s.io/klog/v2 v2.4.0
	k8s.io/kube-openapi v0.0.0-00010101000000-000000000000 // indirect
)

replace (
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1
	google.golang.org/grpc => google.golang.org/grpc v1.54.0
	k8s.io/api => k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20191004105649-b14e3c49469a
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/apiserver => k8s.io/apiserver v0.0.0-20191109015554-8577c320c87f
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20191004110135-b9eb767d2e1a
	k8s.io/client-go => k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191002184608-9779a9fba520
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20191016015547-9213b55ba309
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
	k8s.io/kubernetes => k8s.io/kubernetes v1.14.8
	k8s.io/metrics => k8s.io/metrics v0.0.0-20191004105854-2e8cf7d0888c
	k8s.io/utils => k8s.io/utils v0.0.0-20200327001022-6496210b90e8
)
