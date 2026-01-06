package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Protocol string

const (
	PROTOCOL_ALL                 Protocol = "all"
	PROTOCOL_HIDERA              Protocol = "hi"
	PROTOCOL_FLOW_UPDATING       Protocol = "fu"
	PROTOCOL_EXTREMA_PROPAGATION Protocol = "ep"
	PROTOCOL_RAND_REPORTS        Protocol = "rr"
	PROTOCOL_DIGEST_DIFFUSION    Protocol = "dd"
)

var protocols = []Protocol{PROTOCOL_HIDERA, PROTOCOL_FLOW_UPDATING, PROTOCOL_EXTREMA_PROPAGATION, PROTOCOL_RAND_REPORTS, PROTOCOL_DIGEST_DIFFUSION}

func areJobPlansValid(plans []*JobPlan) bool {
	for _, planGroup := range groupJobPlans(plans) {
		count := planGroup[0].NodesCount
		degree := planGroup[0].AvgDegree
		for _, plan := range planGroup {
			if plan.NodesCount != count || plan.AvgDegree != degree || !isJobPlanValid(*plan) {
				return false
			}
		}
	}

	return true
}

func groupJobPlans(plans []*JobPlan) map[string][]*JobPlan {
	groups := make(map[string][]*JobPlan)
	for _, plan := range plans {
		groups[plan.OverlayGroup] = append(groups[plan.OverlayGroup], plan)
	}
	return groups
}

func isJobPlanValid(plan JobPlan) bool {
	return isProtocolValid(plan.Protocol)
}

func isProtocolValid(protocol Protocol) bool {
	return slices.Contains(append(protocols, PROTOCOL_ALL), protocol)
}

func unwindProtocol(protocol Protocol) []Protocol {
	if protocol == PROTOCOL_ALL {
		return slices.Clone(protocols)
	}
	return []Protocol{protocol}
}

type JobPlan struct {
	OverlayGroup     string   `json:"overlay_group"`
	Protocol         Protocol `json:"protocol"`
	ExperimanetName  string   `json:"exp_name"`
	NodesCount       int      `json:"nodes_count"`
	AvgDegree        int      `json:"avg_degree"`
	LatencyMS        int      `json:"latency"`
	LossPercentage   int      `json:"loss"`
	Repetitions      int      `json:"repeat"`
	StabilizationMS  int      `json:"stabilization_wait"`
	EventWaitMS      int      `json:"event_wait"`
	EventName        string   `json:"event"`
	AfterEventWaitMS int      `json:"end_wait"`
	EnvFile          string   `json:"env_file"`
	Graph            Graph
}

func (jp JobPlan) FullName() string {
	return fmt.Sprintf("%s_%s", jp.ExperimanetName, jp.Protocol)
}

func (jp JobPlan) Submit(cluster string) (*Job, error) {
	jobID, err := submitJob(jp.FullName(), cluster)
	if err != nil {
		return &Job{}, err
	}
	job := &Job{JobPlan: jp, ID: jobID}
	host, err := job.resolveHost()
	if err != nil {
		return &Job{}, err
	}
	job.Host = host
	log.Printf("Job %s %s (%d) submitted.\n", job.ExperimanetName, job.Protocol, job.ID)

	return job, nil
}

func submitJob(experimentName, cluster string) (int, error) {
	remoteCmd := fmt.Sprintf(`
	export LC_ALL=C LANG=C
	oarsub -l "{cluster='%s'}/nodes=1,walltime=12:00" \
		--project %s 'sleep 43200'
	`, cluster, experimentName)

	out, err := executeRemoteCmd(remoteCmd)
	if err != nil {
		return -1, err
	}

	return extractJobID(out)
}

type Job struct {
	JobPlan
	ID   int
	Host string
}

func (job Job) setUpNetwork() error {
	matrix := job.makeLatencyMatrix()

	latencyFilePath := fmt.Sprintf("latency/%d.txt", job.ID)
	err := job.writeLatencyFile(matrix, latencyFilePath)
	if err != nil {
		return err
	}

	os.Setenv("OAR_JOB_ID", strconv.Itoa(job.ID))

	cmd := exec.Command(
		"oar-p2p",
		"net", "up",
		"--addresses", strconv.Itoa(job.NodesCount),
		"--latency-matrix", latencyFilePath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Failed to create P2P network for job %s.\n\t%v\n\t%s\n", job.FullName(), err, string(stderr.Bytes()))
	}

	return job.addNetworkLoss()
}

func (job Job) makeLatencyMatrix() [][]int {
	matrix := make([][]int, job.NodesCount)
	for i := range matrix {
		matrix[i] = make([]int, job.NodesCount)
		for j := range matrix[i] {
			if i != j {
				matrix[i][j] = job.LatencyMS
			}
		}
	}
	return matrix
}

func (job Job) writeLatencyFile(matrix [][]int, path string) error {
	var sb strings.Builder
	for i := 0; i < job.NodesCount; i++ {
		for j := 0; j < job.NodesCount; j++ {
			sb.WriteString(strconv.Itoa(matrix[i][j]))
			if j < job.NodesCount-1 {
				sb.WriteString(" ")
			}
		}
		if i < job.NodesCount-1 {
			sb.WriteString("\n")
		}
	}
	return os.WriteFile(path, []byte(sb.String()), 0666)
}

func (job Job) addNetworkLoss() error {
	script := fmt.Sprintf(`
	docker run --rm --net=host --privileged local/oar-p2p-networking bash -lc '
	set -e
	for IF in bond0 lo; do
	while read -r line; do
		if [[ "$line" =~ qdisc[[:space:]]netem[[:space:]]([0-9]+):.*delay[[:space:]]([0-9]+)ms ]]; then
		handle="${BASH_REMATCH[1]}"
		latency_val="${BASH_REMATCH[2]}"
		classid=$((handle - 1))
		tc qdisc change dev "${IF}" parent 1:${classid} handle ${handle}: netem delay ${latency_val}ms loss %d%% || true
		fi
	done < <(tc qdisc show dev "${IF}")
	done
	'
	`, job.LossPercentage)

	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", job.Host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(script)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to apply loss on %s for job %s: %w",
			job.Host, job.FullName(), err)
	}

	return nil
}

func (job Job) resolveHost() (string, error) {
	remoteCmd := fmt.Sprintf(`
	export LC_ALL=C LANG=C
	oarstat -J -fj %d
	`, job.ID)

	out, err := executeRemoteCmd(remoteCmd)
	if err != nil {
		return "", err
	}

	return extractHost(out)
}

func (job Job) runExperiment(wg *sync.WaitGroup) {
	err := job.writeExperimentEnvFile()
	if err != nil {
		log.Println(err)
		wg.Done()
		return
	}

	for i := range job.Repetitions {
		repetition := i + 1
		log.Printf("Running experiment %s - repetition %d\n", job.FullName(), repetition)

		err := job.runExperimentRepetition(repetition)
		if err != nil {
			log.Printf("Experiment %s error: %v\n", job.FullName(), err)
		}
	}
	wg.Done()
}

func (job Job) writeExperimentEnvFile() error {
	experimentDirPath := fmt.Sprintf(
		"%s/%s",
		EXPERIMENT_DATA_BASE_PATH,
		job.FullName(),
	)

	var script strings.Builder
	script.WriteString("set -e\n\n")
	script.WriteString(fmt.Sprintf(
		"rm -rf %s && mkdir -p %s\n",
		experimentDirPath, experimentDirPath,
	))

	if job.EnvFile == "" {
		return nil
	}

	env, err := os.ReadFile(job.EnvFile)
	if err != nil {
		return err
	}

	script.WriteString(fmt.Sprintf(
		"cat <<'EOF' > %s/.env\n%s\nEOF\n",
		experimentDirPath,
		env,
	))

	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", job.Host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(script.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to write env file for experiment %s: %w",
			job.FullName(), err,
		)
	}

	return nil
}

func (job Job) runExperimentRepetition(repetition int) error {
	err := startExperiment(job, repetition)
	if err != nil {
		err2 := stopExperiment(job)
		return errors.Join(err, err2)
	}

	time.Sleep(60 * time.Second)

	// todo:  waits, events, save metadata etc.

	return stopExperiment(job)
}

func (job Job) getIPs() []string {
	os.Setenv("OAR_JOB_ID", strconv.Itoa(job.ID))

	cmd := exec.Command("oar-p2p", "net", "show")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		log.Println(err)
		return []string{}
	}

	var IPs []string
	lines := strings.Split(out.String(), "\n")

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		IPs = append(IPs, fields[len(fields)-1])
	}

	return IPs
}
