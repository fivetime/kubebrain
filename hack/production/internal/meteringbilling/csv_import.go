package meteringbilling

import (
	"encoding/csv"
	"fmt"
	"io"
)

const (
	maxMeteringBillingCSVBytes       = 16 << 20
	maxMeteringBillingCSVDataRecords = 100_000
)

func newMeteringBillingCSVReader(reader io.Reader) (*csv.Reader, *io.LimitedReader) {
	limited := &io.LimitedReader{R: reader, N: maxMeteringBillingCSVBytes + 1}
	csvReader := csv.NewReader(limited)
	csvReader.FieldsPerRecord = -1
	return csvReader, limited
}

func readMeteringBillingCSVRecord(
	csvReader *csv.Reader,
	limited *io.LimitedReader,
	name string,
) ([]string, error) {
	record, err := csvReader.Read()
	if limited.N <= 0 {
		return nil, fmt.Errorf("%s CSV exceeds %d bytes", name, maxMeteringBillingCSVBytes)
	}
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read %s CSV: %w", name, err)
	}
	return record, nil
}
