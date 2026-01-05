package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FRONTEND_HOSTNAME         = "nova_cluster"
	HOSTNAME                  = "tamara"
	JOB_STATE_RUNNING         = "R"
	EXPERIMENT_DATA_BASE_PATH = "/home/tamara/experiments/results"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run . <plan-file> <cluster>")
	}

	planFilePath := os.Args[1]
	cluster := os.Args[2]

	exportEnvVars()

	plans := loadJobPlans(planFilePath)

	jobs, err := submitJobs(plans, cluster)
	if err != nil {
		log.Fatal(err)
	}

	waitJobsState(jobs, JOB_STATE_RUNNING, 5, 12)

	setUpNetwork(jobs)

	runExperiments(jobs)

	terminateAllJobs(jobs)
}

func exportEnvVars() {
	os.Setenv("FRONTEND_HOSTNAME", FRONTEND_HOSTNAME)
	os.Setenv("HOSTNAME", HOSTNAME)
}

func loadJobPlans(path string) []*JobPlan {
	log.Println("*** Loading job plans ***")

	jobPlansJson, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	jobPlans := make([]*JobPlan, 0)
	err = json.Unmarshal(jobPlansJson, &jobPlans)
	if err != nil {
		log.Fatal(err)
	}

	if !areJobPlansValid(jobPlans) {
		log.Fatal("job plans invalid")
	}

	attachGraphs(jobPlans)

	return unwindPlans(jobPlans)
}

func attachGraphs(plans []*JobPlan) {
	for _, planGroup := range groupJobPlans(plans) {
		count := planGroup[0].NodesCount
		degree := planGroup[0].AvgDegree
		graph := BuildGraph(count, degree)
		for i := range planGroup {
			planGroup[i].Graph = *graph
		}
	}
}

func unwindPlans(plans []*JobPlan) []*JobPlan {
	unwound := make([]*JobPlan, 0)
	for _, plan := range plans {
		for _, protocol := range unwindProtocol(plan.Protocol) {
			cp := *plan
			cp.Protocol = protocol
			unwound = append(unwound, &cp)
		}
	}
	return unwound
}

func submitJobs(plans []*JobPlan, cluster string) ([]*Job, error) {
	log.Println("*** Submitting jobs ***")

	jobs := []*Job{}
	for _, plan := range plans {
		job, err := plan.Submit(cluster)
		if err != nil {
			err2 := terminateAllJobs(jobs)
			return []*Job{}, errors.Join(err, err2)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func waitJobsState(jobs []*Job, state string, intervalS int, retry int) {
	log.Println("*** Waiting jobs ***")

	remoteCmd := `
		export LC_ALL=C LANG=C
		oarstat -u $USER
	`
	for range retry {
		time.Sleep(time.Duration(intervalS) * time.Second)

		out, err := executeRemoteCmd(remoteCmd)
		if err != nil {
			log.Println(err)
			continue
		}

		states := extractJobStates(out)
		if allEqual(states, state) {
			log.Printf("All jobs in state %s, done!\n", state)
			return
		}

		log.Printf("Waiting for jobs to be in the state %s, sleeping for %ds ...\n", state, intervalS)
	}
	err := terminateAllJobs(jobs)
	if err != nil {
		log.Println(err)
	}
	log.Fatalf("Wait job state %s: max attempts exceeded, exiting ...\n", state)
}

func setUpNetwork(jobs []*Job) {
	for _, job := range jobs {
		err := job.setUpNetwork()
		if err != nil {
			err2 := terminateAllJobs(jobs)
			log.Fatal(errors.Join(err, err2))
		}
		log.Printf("Network set up for job %s: nodes=%d, latency=%dms, loss=%d%%\n", job.FullName(), job.NodesCount, job.LatencyMS, job.LossPercentage)
	}
}

func runExperiments(jobs []*Job) {
	err := buildImages(jobs[0].Host)
	if err != nil {
		log.Fatal(err)
	}
	
	wg := &sync.WaitGroup{}
	for _, job := range jobs {
		wg.Add(1)
		go job.runExperiment(wg)
	}
	wg.Wait()
}

func buildImages(host string) error {
	log.Println("*** Building container images ***")

	scriptBuilder := strings.Builder{}
	scriptBuilder.WriteString(
		"cd /home/tamara/hidera && docker build -t hidera:latest /home/tamara/hidera\n",
	)
	// todo: add others

	cmd := exec.Command(
		"ssh", FRONTEND_HOSTNAME,
		"ssh", "-o", "StrictHostKeyChecking=no", host, "bash", "-s",
	)

	cmd.Stdin = bytes.NewBufferString(scriptBuilder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to build container images: %w", err)
	}
	return nil
}

func terminateAllJobs(jobs []*Job) error {
	log.Println("*** Terminating all jobs ***")

	for _, job := range jobs {
		fmt.Printf("Processing job %s...\n", job.FullName())

		if err := os.Setenv("OAR_JOB_ID", strconv.Itoa(job.ID)); err != nil {
			return err
		}

		fmt.Printf("Bringing down local P2P network for job %s...\n", job.FullName())

		cmd := exec.Command("oar-p2p", "net", "down")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to bring down P2P network for job %s: %w", job.FullName(), err)
		}

		fmt.Printf("Deleting OAR job %s...\n", job.FullName())

		delCmd := exec.Command(
			"ssh",
			FRONTEND_HOSTNAME,
			"oardel", strconv.Itoa(job.ID),
		)
		delCmd.Stdout = nil
		delCmd.Stderr = nil

		if err := delCmd.Run(); err != nil {
			fmt.Printf("Failed to delete job %s remotely\n", job.FullName())
		}
	}
	return nil
}
