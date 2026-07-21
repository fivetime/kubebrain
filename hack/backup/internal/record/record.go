package record

type Record struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	ModRevision    int64  `json:"mod_revision"`
	CreateRevision int64  `json:"create_revision"`
	Version        int64  `json:"version"`
	Lease          int64  `json:"lease"`
}

type Lease struct {
	Type       string `json:"type"`
	ID         int64  `json:"id"`
	TTL        int64  `json:"ttl"`
	GrantedTTL int64  `json:"granted_ttl,omitempty"`
}
