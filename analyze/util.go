package main

import (
	"encoding/csv"
	"os"
)

func dirExists(p string) bool {
	i, err := os.Stat(p)
	return err == nil && i.IsDir()
}

func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

func mapKeysInt64[M ~map[int64]V, V any](m M) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func containsString(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
