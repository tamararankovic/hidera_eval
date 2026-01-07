package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var eventFns = map[string]func(Job) []EventMetadata{
	"noop":                  NoopEvent,
	"kill_percent":          KillPercentEvent, // params: percent
	"kill_root":             KillRootEvent,
	"edit_input_once":       EditInputOnce,       // params: percent
	"edit_input_continuous": EditInputContinuous, // params: interval, total_edits
}

func NoopEvent(job Job) []EventMetadata {
	return []EventMetadata{}
}

func KillPercentEvent(job Job) []EventMetadata {
	events := []EventMetadata{}

	selected := selectPercentageOfNodes(job)

	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString("set -e\n\n")

	for _, nodeID := range selected {
		scriptBuilder.WriteString(fmt.Sprintf("docker kill node_%d\n", nodeID))
	}

	cmd := exec.Command(
		"ssh",
		"-T",
		"-J", FRONTEND_HOSTNAME,
		"-o", "StrictHostKeyChecking=no",
		job.Host,
		"bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("failed to kill containers in experiment %s: %v", job.FullName(), err)
		return events
	}

	ts := time.Now().UnixNano()

	event := EventMetadata{
		EventTs:       ts,
		ExpectedValue: computeExpectedValue(job.NodesCount, selected),
		ExcludeNodes:  nodeIDsToNames(selected),
	}
	events = append(events, event)

	return events
}

func KillRootEvent(job Job) []EventMetadata {
	events := []EventMetadata{}

	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString("set -e\n\n")

	nodeID := job.NodesCount
	scriptBuilder.WriteString(fmt.Sprintf("docker kill node_%d\n", nodeID))

	cmd := exec.Command(
		"ssh",
		"-T",
		"-J", FRONTEND_HOSTNAME,
		"-o", "StrictHostKeyChecking=no",
		job.Host,
		"bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Stdin = bytes.NewBufferString(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("failed to kill containers in experiment %s: %v", job.FullName(), err)
		return events
	}

	ts := time.Now().UnixNano()

	selected := []int{nodeID}
	event := EventMetadata{
		EventTs:       ts,
		ExpectedValue: computeExpectedValue(job.NodesCount, selected),
		ExcludeNodes:  nodeIDsToNames(selected),
	}
	events = append(events, event)

	return events
}

const metricsTemplate = `
# HELP app_memory_usage_bytes Current memory usage in bytes
# TYPE app_memory_usage_bytes gauge
app_memory_usage_bytes %d
`

// na % cvorova jednom
func EditInputOnce(job Job) []EventMetadata {
	events := []EventMetadata{}

	nodeIDs := selectPercentageOfNodes(job)
	nodeNames := []string{}
	for _, id := range nodeIDs {
		nodeNames = append(nodeNames, fmt.Sprintf("node_%d", id))
	}

	IPs, err := discoverIPs(job.Host, nodeNames)
	if err != nil {
		log.Println(err)
		return events
	}

	event := editInput(IPs, nodeIDs, job, 2)
	if event != nil {
		events = append(events, *event)
	}

	return events
}

// na svima svakih n sekundi, m puta
func EditInputContinuous(job Job) []EventMetadata {
	events := []EventMetadata{}

	nodeIDs := []int{}
	for i := range job.NodesCount {
		nodeIDs = append(nodeIDs, i+1)
	}
	nodeNames := []string{}
	for _, id := range nodeIDs {
		nodeNames = append(nodeNames, fmt.Sprintf("node_%d", id))
	}

	intervalStr := job.EventParams["interval"]
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Println(err)
		return events
	}
	totalEditsStr := job.EventParams["total_edits"]
	totalEdits, err := strconv.Atoi(totalEditsStr)
	if err != nil {
		log.Println(err)
		return events
	}

	IPs, err := discoverIPs(job.Host, nodeNames)
	if err != nil {
		log.Println(err)
		return events
	}

	for i := range totalEdits {
		event := editInput(IPs, nodeIDs, job, i+2)
		if event != nil {
			events = append(events, *event)
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}

	return events
}

func editInput(IPs []string, nodeIDs []int, job Job, multiplier int) *EventMetadata {
	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString("set -e\n\n")

	newValues := map[int]int{}

	for i, ip := range IPs {
		mem := multiplier * (i+1)
		newValues[nodeIDs[i]] = mem
		scriptBuilder.WriteString(fmt.Sprintf(`
curl -s -X POST -H 'Content-Type: text/plain' \
  --data-binary @- "http://%s:9200/metrics" <<'METRICS'
%s
METRICS

`, ip, fmt.Sprintf(metricsTemplate, mem)))
	}

	cmd := exec.Command(
		"ssh",
		"-T",
		"-J", FRONTEND_HOSTNAME,
		job.Host,
		"bash", "-s",
	)

	cmd.Stdin = strings.NewReader(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("failed to post metrics: %v", err)
		return nil
	}

	ts := time.Now().UnixNano()

	count := float64(job.NodesCount)
	sum := count * (count + 1) / 2
	for _, id := range nodeIDs {
		sum += float64(newValues[id]) - float64(id)
	}
	expected := sum / count

	return &EventMetadata{
		EventTs:       ts,
		ExpectedValue: expected,
		ExcludeNodes:  []string{},
	}
}

func computeExpectedValue(count int, excludedNodeIDs []int) float64 {
	c := float64(count)
	sum := c * (c + 1) / 2
	for _, nodeID := range excludedNodeIDs {
		sum -= float64(nodeID)
		c--
	}
	return sum / c
}

func nodeIDsToNames(IDs []int) []string {
	names := []string{}
	for _, id := range IDs {
		names = append(names, fmt.Sprintf("node_%d", id))
	}
	return names
}

func discoverIPs(host string, containerNames []string) ([]string, error) {
	if len(containerNames) == 0 {
		return nil, nil
	}

	script := strings.Builder{}
	script.WriteString("set -euo pipefail\n\n")

	for _, name := range containerNames {
		script.WriteString(fmt.Sprintf(`
docker inspect %s --format '{{json .Config.Env}}' | sed 's/^/%s\t/'
`, name, name))
	}

	cmd := exec.Command(
		"ssh",
		"-T",
		"-J", FRONTEND_HOSTNAME,
		host,
		"bash", "-s",
	)

	cmd.Stdin = strings.NewReader(script.String())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf(
			"discoverIPs failed: %w\nstderr:\n%s",
			err,
			stderr.String(),
		)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	ips := make([]string, 0, len(containerNames))

	for _, line := range lines {
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}

		var env []string
		if err := json.Unmarshal([]byte(parts[1]), &env); err != nil {
			return nil, fmt.Errorf("failed to parse env JSON: %w\nline=%q", err, line)
		}

		for _, e := range env {
			if strings.HasPrefix(e, "LISTEN_IP=") {
				ips = append(ips, strings.TrimPrefix(e, "LISTEN_IP="))
				break
			}
		}
	}

	if len(ips) != len(containerNames) {
		return nil, fmt.Errorf(
			"expected %d IPs, got %d\nraw output:\n%s",
			len(containerNames),
			len(ips),
			stdout.String(),
		)
	}

	return ips, nil
}

func selectPercentageOfNodes(job Job) []int {
	selected := []int{}
	percentStr := job.EventParams["percent"]
	percent, err := strconv.Atoi(percentStr)
	if err != nil {
		log.Println(err)
		return selected
	}

	candidates := []int{}
	for i := range job.NodesCount {
		id := i + 1
		if id == 1 || id == job.NodesCount {
			continue
		}
		candidates = append(candidates, id)
	}

	step := max(100/percent, 1)
	for i := 0; i < len(candidates); i += step {
		selected = append(selected, candidates[i])
	}
	return selected
}
