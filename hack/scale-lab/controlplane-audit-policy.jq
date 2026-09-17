# Request/response bodies are limited to disposable fixtures, never credentials.
{apiVersion:"audit.k8s.io/v1",kind:"Policy",omitStages:["RequestReceived"],rules:
  ((if $kwok then [
    {level:"RequestResponse",users:["kubebrain-test-kwok"],verbs:["get","list"],
      namespaces:["kube-node-lease"],resources:[{group:"coordination.k8s.io",resources:["leases"]}]},
    {level:"RequestResponse",users:["kubebrain-test-kwok"],verbs:["patch","update"],
      resources:[{group:"",resources:["nodes/status","pods/status"]},{group:"coordination.k8s.io",resources:["leases"]}]}
  ] else [] end) + [
    {level:"RequestResponse",verbs:["create"],namespaces:["controlplane-smoke"],
      resources:[{group:"apps",resources:["replicasets"]},{group:"",resources:["pods"]}]},
    {level:"Metadata"}
  ])}
