package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("usage: %s <matrix_dimension> <latency>", os.Args[0])
	}

	d, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	latency, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	// Create d x d matrix
	matrix := make([][]int, d)
	for i := range matrix {
		matrix[i] = make([]int, d)
		for j := range matrix[i] {
			if i != j {
				matrix[i][j] = latency
			}
		}
	}

	// Write to file
	var sb strings.Builder
	for i := 0; i < d; i++ {
		for j := 0; j < d; j++ {
			sb.WriteString(strconv.Itoa(matrix[i][j]))
			if j < d-1 {
				sb.WriteString(" ")
			}
		}
		if i < d-1 {
			sb.WriteString("\n")
		}
	}

	fmt.Println(sb.String())
}
