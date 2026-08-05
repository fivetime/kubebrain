package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyStorageCapacity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:  "equivalent binary quantities",
			input: `{"claims":[{"name":"data","pv":"pv-data","requested_storage":"1Gi"}],"pvs":[{"name":"pv-data","capacity":"1024Mi"}]}`,
		},
		{
			name:  "larger decimal capacity",
			input: `{"claims":[{"name":"data","pv":"pv-data","requested_storage":"1Gi"}],"pvs":[{"name":"pv-data","capacity":"2G"}]}`,
		},
		{
			name:    "undersized",
			input:   `{"claims":[{"name":"data","pv":"pv-data","requested_storage":"1Gi"}],"pvs":[{"name":"pv-data","capacity":"1023Mi"}]}`,
			wantErr: `PV "pv-data" capacity 1023Mi is smaller than PVC "data" request 1Gi`,
		},
		{
			name:    "invalid quantity",
			input:   `{"claims":[{"name":"data","pv":"pv-data","requested_storage":"1Gi"}],"pvs":[{"name":"pv-data","capacity":"huge"}]}`,
			wantErr: `invalid capacity for PV "pv-data"`,
		},
		{
			name:    "bounded input",
			input:   strings.Repeat(" ", maxInventoryBytes+1),
			wantErr: "storage inventory exceeds 4 MiB",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := verify(strings.NewReader(tc.input))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}
