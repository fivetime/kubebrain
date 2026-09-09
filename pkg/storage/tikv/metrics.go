package tikv

import clientmetrics "github.com/tikv/client-go/v2/metrics"

func init() {
	// client-go initializes and observes these collectors, but deliberately
	// leaves registration to its embedding application. KubeBrain serves the
	// default Prometheus registry. Register once at package initialization, not
	// once per pooled client or storage instance, and never reinitialize the
	// collectors after client-go has cached their child observers.
	clientmetrics.RegisterMetrics()
}
