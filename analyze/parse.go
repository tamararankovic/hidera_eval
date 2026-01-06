package main

import (
	"fmt"
	"log"
	"strconv"
)

func csvToValueRows(csvRows [][]string) []*ValueRow {
	rows := make([]*ValueRow, 0)
	for _, csvRow := range csvRows {
		row, err := parseValueRow(csvRow)
		if err != nil {
			log.Println(err)
			continue
		}
		rows = append(rows, &row)
	}
	return rows
}

func csvToMsgCountRows(csvRows [][]string) []*MsgCountRow {
	rows := make([]*MsgCountRow, 0)
	for _, csvRow := range csvRows {
		row, err := parseMsgCountRow(csvRow)
		if err != nil {
			log.Println(err)
			continue
		}
		rows = append(rows, &row)
	}
	return rows
}

func parseValueRow(rec []string) (ValueRow, error) {
	if len(rec) < 4 {
		return ValueRow{}, fmt.Errorf("expected at least 4 columns, got %d", len(rec))
	}

	ts, err := strconv.ParseInt(rec[2], 10, 64)
	if err != nil {
		return ValueRow{}, fmt.Errorf("invalid timestamp %q: %w", rec[2], err)
	}

	val, err := strconv.ParseFloat(rec[3], 64)
	if err != nil {
		return ValueRow{}, fmt.Errorf("invalid value %q: %w", rec[3], err)
	}

	return ValueRow{
		Timestamp: ts,
		Value:     val,
	}, nil
}

func parseMsgCountRow(rec []string) (MsgCountRow, error) {
	if len(rec) < 3 {
		return MsgCountRow{}, fmt.Errorf("expected at least 3 columns, got %d", len(rec))
	}

	ts, err := strconv.ParseInt(rec[0], 10, 64)
	if err != nil {
		return MsgCountRow{}, fmt.Errorf("invalid timestamp %q: %w", rec[0], err)
	}

	sent, err := strconv.ParseInt(rec[1], 10, 64)
	if err != nil {
		return MsgCountRow{}, fmt.Errorf("invalid sent %q: %w", rec[1], err)
	}

	rcvd, err := strconv.ParseInt(rec[2], 10, 64)
	if err != nil {
		return MsgCountRow{}, fmt.Errorf("invalid rcvd %q: %w", rec[2], err)
	}

	return MsgCountRow{
		Timestamp: ts,
		Sent:      sent,
		Rcvd:      rcvd,
	}, nil
}