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
)

var protocolNames = map[Protocol]string{
	PROTOCOL_HIDERA:              "hidera",
	PROTOCOL_FLOW_UPDATING:       "flow_updating",
	PROTOCOL_EXTREMA_PROPAGATION: "extrema_propagation",
	PROTOCOL_RAND_REPORTS:        "rand_reports",
	PROTOCOL_DIGEST_DIFFUSION:    "digest_diffusion",
}

var protocolDirs = map[Protocol]string{
	PROTOCOL_HIDERA:              "hidera",
	PROTOCOL_FLOW_UPDATING:       "flow_updating",
	PROTOCOL_EXTREMA_PROPAGATION: "extrema_propagation",
	PROTOCOL_RAND_REPORTS:        "randomized_reports",
	PROTOCOL_DIGEST_DIFFUSION:    "digest_diffusion",
}

func startExperiment(job Job, repetition int) error {
	protocolName := protocolNames[job.Protocol]

	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString("set -e\n\n")

	IPs := job.getIPs()

	for containerIdx := range job.NodesCount {
		ip := IPs[containerIdx]
		id := containerIdx + 1
		name := fmt.Sprintf("%s_%d", protocolName, id)
		logDirPath := fmt.Sprintf("%s/%s/exp_%d/%s", EXPERIMENT_DATA_BASE_PATH, job.FullName(), repetition, name)
		envFilePath := fmt.Sprintf("%s/%s/.env", EXPERIMENT_DATA_BASE_PATH, job.FullName())
		peerIDs := []string{}
		peerIPs := []string{}
		for _, peerContainerIdx := range job.Graph.Adj[containerIdx] {
			peerID := peerContainerIdx + 1
			peerIDs = append(peerIDs, strconv.Itoa(peerID))
			peerIPs = append(peerIPs, IPs[peerContainerIdx])
		}

		scriptBuilder.WriteString(fmt.Sprintf("mkdir -p %s\n", logDirPath))
		scriptBuilder.WriteString(
			fmt.Sprintf("docker rm -f %s >/dev/null 2>&1 || true\n", name),
		)
		scriptBuilder.WriteString(fmt.Sprintf(`
docker run -d \
--name %s \
--hostname %s \
--network host \
--memory 250m \
-e ID=%d \
-e LISTEN_IP=%s \
-e LISTEN_PORT=9000 \
-e PEER_IDS=%s \
-e PEER_IPS=%s \
--env-file "%s" \
-v "%s:/var/log/%s" \
%s:latest

`, name, name, id, ip,
			strings.Join(peerIDs, ","),
			strings.Join(peerIPs, ","),
			envFilePath,
			logDirPath,
			protocolName,
			protocolName,
		))
	}

	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", job.Host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start experiment %s: %w",
			job.FullName(), err)
	}

	return nil
}

func stopExperiment(job Job) error {
	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", job.Host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(`
	docker ps -a -q | xargs -r docker stop
	docker ps -a -q | xargs -r docker rm
	`)
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop experiment %s: %w",
			job.FullName(), err)
	}
	return nil
}

type ExperimentRunMetadata struct {
	Job               Job             `json:"job"`
	Repetition        int             `json:"repetition"`
	StartExperimentTs int64           `json:"exp_start_ts"`
	StartEventsTs     int64           `json:"events_start_ts"`
	StopEventsTs      int64           `json:"events_stop_ts"`
	StopExperimentTs  int64           `json:"exp_stop_ts"`
	Events            []EventMetadata `json:"events"`
}

type EventMetadata struct {
	EventTs       int64    `json:"event_ts"`
	ExpectedValue float64  `json:"expected_value"`
	IncludeNodes  []string `json:"include_nodes"`
	ExcludeNodes  []string `json:"exclude_nodes"`
}

func saveExperimentRunMetadata(metadata ExperimentRunMetadata) {
	metadataJson, err := json.Marshal(&metadata)
	if err != nil {
		log.Println(err)
		return
	}

	metadataFilePath := fmt.Sprintf("%s/%s/exp_%d/metadata.json", EXPERIMENT_DATA_BASE_PATH, metadata.Job.FullName(), metadata.Repetition)
	var script strings.Builder
	script.WriteString("set -e\n\n")

	script.WriteString(fmt.Sprintf(
		"cat <<'EOF' > %s\n%s\nEOF\n",
		metadataFilePath,
		string(metadataJson),
	))

	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", metadata.Job.Host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(script.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("failed to write metadata file for experiment %s: %v\n", metadata.Job.FullName(), err)
	}
}
