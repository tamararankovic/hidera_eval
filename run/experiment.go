package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var startExperimentFns = map[Protocol]func(job Job, repetition int) error{
	PROTOCOL_HIDERA:              startHidera,
	PROTOCOL_FLOW_UPDATING:       startFlowUpdating,
	PROTOCOL_EXTREMA_PROPAGATION: startExtremaPropagation,
	PROTOCOL_RAND_REPORTS:        startRandReports,
	PROTOCOL_DIGEST_DIFFUSION:    startDigestDiffusion,
}

func startHidera(job Job, repetition int) error {
	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString("set -e\n\n")

	IPs := job.getIPs()

	for containerIdx := range job.NodesCount {
		ip := IPs[containerIdx]
		id := containerIdx + 1
		name := fmt.Sprintf("hidera_%d", id)
		logDirPath := fmt.Sprintf("%s/%s/exp_%d/%s", EXPERIMENT_DATA_BASE_PATH, job.FullName(), repetition, name)
		envFilePath := fmt.Sprintf("%s/%s/.env", EXPERIMENT_DATA_BASE_PATH, job.FullName())
		neighborIDs := []string{}
		neighborIPs := []string{}
		for _, neighborContainerIdx := range job.Graph.Adj[containerIdx] {
			neighborID := neighborContainerIdx + 1
			neighborIDs = append(neighborIDs, strconv.Itoa(neighborID))
			neighborIPs = append(neighborIPs, IPs[neighborContainerIdx])
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
-e LISTEN_HOST=%s \
-e ID=%d \
-e LISTEN_PORT=9000 \
-e PEER_IDS=%s \
-e PEER_IPS=%s \
--env-file "%s" \
-v "%s:/var/log/hidera" \
hidera:latest

`, name, name, ip, id,
			strings.Join(neighborIDs, ","),
			strings.Join(neighborIPs, ","),
			envFilePath,
			logDirPath,
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

func startFlowUpdating(job Job, repetition int) error {
	// todo: add
	return nil
}

func startExtremaPropagation(job Job, repetition int) error {
	// todo: add
	return nil
}

func startRandReports(job Job, repetition int) error {
	// todo: add
	return nil
}

func startDigestDiffusion(job Job, repetition int) error {
	// todo: add
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

// todo: exec cmd is all over the place
