package leasefault

import (
	"net/http"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// NewDynamicClient disables client-go request retries, including Retry-After
// responses to mutations. An uncertain fault mutation must be reconciled, never
// silently dispatched again. Callers still own TLS, endpoint and credential
// admission; injected transports must not implement their own mutation retries.
func NewDynamicClient(config *rest.Config) (dynamic.Interface, error) {
	c := dynamic.ConfigFor(config)
	c.GroupVersion = nil
	c.APIPath = "/"
	httpClient, err := rest.HTTPClientFor(c)
	if err != nil {
		return nil, err
	}
	// A 307/308 redirect can replay a mutation outside client-go's retry loop.
	// This tool admits one direct API endpoint and must not follow redirects.
	// HTTPClientFor may return http.DefaultClient; never modify shared state.
	privateClient := *httpClient
	httpClient = &privateClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client, err := rest.UnversionedRESTClientForConfigAndClient(c, httpClient)
	if err != nil {
		return nil, err
	}
	return dynamic.New(singleAttemptREST{client}), nil
}

type singleAttemptREST struct{ rest.Interface }

func (c singleAttemptREST) Verb(verb string) *rest.Request {
	return c.Interface.Verb(verb).MaxRetries(0)
}
func (c singleAttemptREST) Post() *rest.Request { return c.Interface.Post().MaxRetries(0) }
func (c singleAttemptREST) Put() *rest.Request  { return c.Interface.Put().MaxRetries(0) }
func (c singleAttemptREST) Get() *rest.Request  { return c.Interface.Get().MaxRetries(0) }
func (c singleAttemptREST) Delete() *rest.Request {
	return c.Interface.Delete().MaxRetries(0)
}
func (c singleAttemptREST) Patch(pt types.PatchType) *rest.Request {
	return c.Interface.Patch(pt).MaxRetries(0)
}
