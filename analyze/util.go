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
