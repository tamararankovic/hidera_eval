package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const EXPERIMENT_DATA_BASE_PATH = "/home/tamara/experiments"

// const EXPERIMENT_DATA_BASE_PATH = "/Users/tamararankovic/Documents/monitoring/impl/hidera_eval/tmp"

var protocols = []string{"hi", "fu", "ep", "dd", "rr"}

var experimentName = ""
var dirPath = ""

type ValueRow struct {
	Timestamp int64
	Value     float64
}

type MsgCountRow struct {
	Timestamp int64
	Sent      int64
	Rcvd      int64
}

type RepetitionData struct {
	Metadata *ExperimentRunMetadata
	Nodes    map[string]*NodeData
}

type NodeData struct {
	Values    []*ValueRow
	MsgCounts []*MsgCountRow
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run . <experiment-name>")
	}

	experimentName = os.Args[1]
	dirPath = fmt.Sprintf("%s/%s_analyzed", EXPERIMENT_DATA_BASE_PATH, experimentName)
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		log.Println(err)
		return
	}

	files := findExperimentFiles()
	data := loadExperimentData(files)
	preprocess(data)

	makeExpectedValueSeries(data)

	makeValuesSeries(data)

	makeMsgCountAndRate(data)
}

func findExperimentFiles() map[string]map[string][]string {
	experimentData := map[string]map[string][]string{}

	for _, protocol := range protocols {
		experimentDirPath := fmt.Sprintf("%s/%s_%s", EXPERIMENT_DATA_BASE_PATH, experimentName, protocol)

		if !dirExists(experimentDirPath) {
			continue
		}

		protocolData := make(map[string][]string)

		repetitions, err := os.ReadDir(experimentDirPath)
		if err != nil {
			log.Println(err)
			continue
		}

		for _, repetition := range repetitions {
			if !repetition.IsDir() || !strings.HasPrefix(repetition.Name(), "exp_") {
				continue
			}
			protocolData[repetition.Name()] = make([]string, 0)
			nodes, err := os.ReadDir(fmt.Sprintf("%s/%s", experimentDirPath, repetition.Name()))
			if err != nil {
				log.Println(err)
				continue
			}
			for _, node := range nodes {
				if !node.IsDir() || !strings.HasPrefix(node.Name(), "node_") {
					continue
				}
				nodePath := fmt.Sprintf("%s/%s/%s", experimentDirPath, repetition.Name(), node.Name())
				protocolData[repetition.Name()] = append(protocolData[repetition.Name()], nodePath)
			}
		}

		experimentData[fmt.Sprintf("%s_%s", experimentName, protocol)] = protocolData
	}
	return experimentData
}

func loadExperimentData(files map[string]map[string][]string) map[string]map[string]*RepetitionData {
	data := make(map[string]map[string]*RepetitionData)
	for protocol, repetitions := range files {
		data[protocol] = make(map[string]*RepetitionData)
		for repetition, nodes := range repetitions {
			metadataJson, err := os.ReadFile(fmt.Sprintf("%s/%s/%s/metadata.json", EXPERIMENT_DATA_BASE_PATH, protocol, repetition))
			if err != nil {
				log.Println(err)
				continue
			}
			var metadata ExperimentRunMetadata
			err = json.Unmarshal(metadataJson, &metadata)
			if err != nil {
				log.Println(err)
				continue
			}
			data[protocol][repetition] = &RepetitionData{
				Metadata: &metadata,
				Nodes:    make(map[string]*NodeData),
			}
			for _, nodeDirPath := range nodes {
				valuesFilePath := fmt.Sprintf("%s/value.csv", nodeDirPath)
				csvValuesRows, err := readCSV(valuesFilePath)
				if err != nil {
					log.Println(err)
					continue
				}
				msgCountsFilePath := fmt.Sprintf("%s/msg_count.csv", nodeDirPath)
				csvMsgCountsRows, err := readCSV(msgCountsFilePath)
				if err != nil {
					log.Println(err)
					continue
				}
				parts := strings.Split(nodeDirPath, "/")
				nodeName := parts[len(parts)-1]
				node := &NodeData{
					Values:    csvToValueRows(csvValuesRows),
					MsgCounts: csvToMsgCountRows(csvMsgCountsRows),
				}
				data[protocol][repetition].Nodes[nodeName] = node
			}
		}
	}
	return data
}

func preprocess(data map[string]map[string]*RepetitionData) {
	timestampsToSeconds(data)
	normalizeTime(data)
}

func timestampsToSeconds(data map[string]map[string]*RepetitionData) {
	for _, repetitions := range data {
		for _, repetition := range repetitions {
			repetition.Metadata.StartExperimentTs /= 1_000_000_000
			repetition.Metadata.StartEventsTs /= 1_000_000_000
			repetition.Metadata.StopEventsTs /= 1_000_000_000
			repetition.Metadata.StopExperimentTs /= 1_000_000_000
			for _, event := range repetition.Metadata.Events {
				event.EventTs /= 1_000_000_000
			}
			for _, node := range repetition.Nodes {
				for _, row := range node.Values {
					row.Timestamp /= 1_000_000_000
				}
				for _, row := range node.MsgCounts {
					row.Timestamp /= 1_000_000_000
				}
			}
		}
	}
}

func normalizeTime(data map[string]map[string]*RepetitionData) {
	for _, repetitions := range data {
		for _, repetition := range repetitions {
			minTs := repetition.Metadata.StartExperimentTs
			maxTs := repetition.Metadata.StopExperimentTs
			maxNormalizedTs := maxTs - minTs
			repetition.Metadata.StartExperimentTs -= minTs
			repetition.Metadata.StartEventsTs -= minTs
			repetition.Metadata.StopEventsTs -= minTs
			repetition.Metadata.StopExperimentTs -= minTs
			for _, event := range repetition.Metadata.Events {
				event.EventTs -= minTs
			}
			for _, node := range repetition.Nodes {
				filteredValues := make([]*ValueRow, 0)
				for _, row := range node.Values {
					row.Timestamp -= minTs
					if row.Timestamp >= 0 && row.Timestamp <= maxNormalizedTs {
						filteredValues = append(filteredValues, row)
					}
				}
				node.Values = filteredValues
				filteredMsgCounts := make([]*MsgCountRow, 0)
				for _, row := range node.MsgCounts {
					row.Timestamp -= minTs
					if row.Timestamp >= 0 && row.Timestamp <= maxNormalizedTs {
						filteredMsgCounts = append(filteredMsgCounts, row)
					}
				}
				node.MsgCounts = filteredMsgCounts
			}
		}
	}
}

func makeExpectedValueSeries(data map[string]map[string]*RepetitionData) {
	var node *NodeData
	var metadata *ExperimentRunMetadata
	for _, repetitions := range data {
		node, metadata = findReferencePoint(repetitions)
	}
	if node == nil || metadata == nil {
		return
	}

	expectedValues := make([]*ValueRow, 0)
	for _, point := range node.Values {
		value := &ValueRow{
			Timestamp: point.Timestamp,
		}
		activeEvent := findActiveEvent(point.Timestamp, metadata.Events)
		if activeEvent != nil {
			value.Value = activeEvent.ExpectedValue
		} else {
			value.Value = metadata.Job.ExpectedValue
		}
		expectedValues = append(expectedValues, value)
	}
	filename := fmt.Sprintf("%s/value_expected.csv", dirPath)
	writeValuesToCSV(filename, expectedValues)
}

func makeValuesSeries(data map[string]map[string]*RepetitionData) {
	var node *NodeData
	var metadata *ExperimentRunMetadata
	for _, repetitions := range data {
		node, metadata = findReferencePoint(repetitions)
	}
	if node == nil || metadata == nil {
		return
	}

	values := make(map[string]map[string][]*ValueRow)
	averagedValues := make(map[string][]*ValueRow)

	nodeSet := make(map[string]struct{})
	for protocol, repetitions := range data {
		values[protocol] = make(map[string][]*ValueRow)
		averagedValues[protocol] = make([]*ValueRow, 0)
		for _, repetition := range repetitions {
			for name := range repetition.Nodes {
				if _, ok := values[protocol][name]; !ok {
					values[protocol][name] = []*ValueRow{}
				}
				nodeSet[name] = struct{}{}
			}
		}
	}

	for protocol, repetitions := range data {
		totalSum := map[int64]float64{}
		totalCount := map[int64]int64{}
		for nodeName := range nodeSet {

			sum := map[int64]float64{}
			count := map[int64]int64{}

			for _, repetition := range repetitions {
				nodeData, ok := repetition.Nodes[nodeName]
				if !ok {
					continue
				}
				for _, point := range nodeData.Values {
					event := findActiveEvent(point.Timestamp, metadata.Events)
					if event != nil && containsString(event.ExcludeNodes, nodeName) {
						continue
					}
					sum[point.Timestamp] += point.Value
					count[point.Timestamp]++
					totalSum[point.Timestamp] += point.Value
					totalCount[point.Timestamp]++
				}
			}

			timestamps := mapKeysInt64(sum)
			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i] < timestamps[j]
			})

			for _, ts := range timestamps {
				values[protocol][nodeName] = append(
					values[protocol][nodeName],
					&ValueRow{
						Timestamp: ts,
						Value:     sum[ts] / float64(count[ts]),
					},
				)
			}
		}

		timestamps := mapKeysInt64(totalSum)
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		for _, ts := range timestamps {
			averagedValues[protocol] = append(
				averagedValues[protocol],
				&ValueRow{
					Timestamp: ts,
					Value:     totalSum[ts] / float64(totalCount[ts]),
				},
			)
		}
	}

	for protocol, nodes := range values {
		parts := strings.Split(protocol, "_")
		protocolName := parts[len(parts)-1]
		filename := fmt.Sprintf("%s/%s_value_averaged.csv", dirPath, protocolName)
		writeValuesToCSV(filename, averagedValues[protocol])
		for nodeName, value := range nodes {
			filename := fmt.Sprintf("%s/%s_value_%s.csv", dirPath, protocolName, nodeName)
			writeValuesToCSV(filename, value)
		}
	}
}

func makeMsgCountAndRate(data map[string]map[string]*RepetitionData) {
	var node *NodeData
	var metadata *ExperimentRunMetadata

	for _, repetitions := range data {
		node, metadata = findReferencePoint(repetitions)
	}
	if node == nil || metadata == nil {
		return
	}

	msgCounts := make(map[string]map[string][]*MsgCountRow)
	avgMsgCounts := make(map[string][]*MsgCountRow)

	nodeSet := make(map[string]struct{})

	for protocol, repetitions := range data {
		msgCounts[protocol] = make(map[string][]*MsgCountRow)
		avgMsgCounts[protocol] = []*MsgCountRow{}

		for _, repetition := range repetitions {
			for name := range repetition.Nodes {
				nodeSet[name] = struct{}{}
				if _, ok := msgCounts[protocol][name]; !ok {
					msgCounts[protocol][name] = []*MsgCountRow{}
				}
			}
		}
	}

	for protocol, repetitions := range data {

		totalSent := map[int64]int64{}
		totalRcvd := map[int64]int64{}
		totalCount := map[int64]int64{}

		for nodeName := range nodeSet {

			sumSent := map[int64]int64{}
			sumRcvd := map[int64]int64{}
			count := map[int64]int64{}

			for _, repetition := range repetitions {
				nodeData, ok := repetition.Nodes[nodeName]
				if !ok {
					continue
				}

				for _, row := range nodeData.MsgCounts {
					event := findActiveEvent(row.Timestamp, metadata.Events)
					if event != nil && containsString(event.ExcludeNodes, nodeName) {
						continue
					}

					sumSent[row.Timestamp] += row.Sent
					sumRcvd[row.Timestamp] += row.Rcvd
					count[row.Timestamp]++

					totalSent[row.Timestamp] += row.Sent
					totalRcvd[row.Timestamp] += row.Rcvd
					totalCount[row.Timestamp]++
				}
			}

			timestamps := mapKeysInt64(sumSent)
			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i] < timestamps[j]
			})

			for _, ts := range timestamps {
				msgCounts[protocol][nodeName] = append(
					msgCounts[protocol][nodeName],
					&MsgCountRow{
						Timestamp: ts,
						Sent:      sumSent[ts] / count[ts],
						Rcvd:      sumRcvd[ts] / count[ts],
					},
				)
			}
		}

		timestamps := mapKeysInt64(totalSent)
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		for _, ts := range timestamps {
			avgMsgCounts[protocol] = append(
				avgMsgCounts[protocol],
				&MsgCountRow{
					Timestamp: ts,
					Sent:      totalSent[ts] / totalCount[ts],
					Rcvd:      totalRcvd[ts] / totalCount[ts],
				},
			)
		}
	}

	for protocol, nodes := range msgCounts {
		parts := strings.Split(protocol, "_")
		protocolName := parts[len(parts)-1]

		writeMsgCountsToCSV(
			fmt.Sprintf("%s/%s_msgcount_averaged.csv", dirPath, protocolName),
			avgMsgCounts[protocol],
		)

		writeMsgRateToCSV(
			fmt.Sprintf("%s/%s_msgrate_averaged.csv", dirPath, protocolName),
			avgMsgCounts[protocol],
		)

		for nodeName, rows := range nodes {
			writeMsgCountsToCSV(
				fmt.Sprintf("%s/%s_msgcount_%s.csv", dirPath, protocolName, nodeName),
				rows,
			)

			writeMsgRateToCSV(
				fmt.Sprintf("%s/%s_msgrate_%s.csv", dirPath, protocolName, nodeName),
				rows,
			)
		}
	}
}

func findActiveEvent(timestamp int64, events []*EventMetadata) *EventMetadata {
	var active *EventMetadata
	for _, event := range events {
		if event.EventTs < timestamp && (active == nil || event.EventTs > active.EventTs) {
			active = event
		}
	}
	return active
}

func findReferencePoint(experiment map[string]*RepetitionData) (*NodeData, *ExperimentRunMetadata) {
	for _, repetition := range experiment {
		for _, node := range repetition.Nodes {
			return node, repetition.Metadata
		}
	}
	return nil, nil
}

func writeValuesToCSV(filename string, data []*ValueRow) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	w := csv.NewWriter(file)

	for _, point := range data {
		tsStr := strconv.FormatInt(point.Timestamp, 10)
		valStr := strconv.FormatFloat(point.Value, 'f', 2, 64)
		err := w.Write([]string{tsStr, valStr})
		if err != nil {
			log.Println(err)
		}
	}
	w.Flush()
}

func writeMsgCountsToCSV(filename string, data []*MsgCountRow) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	w := csv.NewWriter(file)

	for _, r := range data {
		w.Write([]string{
			strconv.FormatInt(r.Timestamp, 10),
			strconv.FormatInt(r.Sent, 10),
			strconv.FormatInt(r.Rcvd, 10),
		})
	}
	w.Flush()
}

func writeMsgRateToCSV(filename string, data []*MsgCountRow) {
	if len(data) < 2 {
		return
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	w := csv.NewWriter(file)

	for i := 1; i < len(data); i++ {
		dt := data[i].Timestamp - data[i-1].Timestamp
		if dt <= 0 {
			continue
		}

		sentRate := float64(data[i].Sent-data[i-1].Sent) / float64(dt)
		rcvdRate := float64(data[i].Rcvd-data[i-1].Rcvd) / float64(dt)

		w.Write([]string{
			strconv.FormatInt(data[i].Timestamp, 10),
			strconv.FormatFloat(sentRate, 'f', 2, 64),
			strconv.FormatFloat(rcvdRate, 'f', 2, 64),
		})
	}
	w.Flush()
}
