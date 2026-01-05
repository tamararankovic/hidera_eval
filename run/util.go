package main

import (
	"bytes"
	"errors"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

func executeRemoteCmd(remoteCmd string) (string, error) {
	cmd := exec.Command("ssh", FRONTEND_HOSTNAME, remoteCmd)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = nil

	err := cmd.Run()

	return stdout.String(), err
}

func extractJobID(out string) (int, error) {
	re := regexp.MustCompile(`[0-9]+`)
	matches := re.FindAllString(out, -1)

	if len(matches) == 0 {
		return -1, errors.New("no job id found")
	}

	jobIDStr := matches[len(matches)-1]
	return strconv.Atoi(jobIDStr)
}

func extractJobStates(out string) []string {
	lines := strings.Split(out, "\n")
	var states []string

	for i, line := range lines {
		if i < 2 || strings.TrimSpace(line) == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			states = append(states, fields[1])
		}
	}

	return states
}

func extractHost(out string) (string, error) {
	lines := strings.Split(out, "\n")
	var address string

	for i, line := range lines {
		if strings.Contains(line, `"assigned_network_address"`) && i+1 < len(lines) {
			next := strings.TrimSpace(lines[i+1])
			next = strings.Trim(next, `" ,`)
			address = next
			break
		}
	}

	if address == "" {
		return "", errors.New("assigned_network_address not found")
	}

	return address, nil
}

func allEqual(states []string, state string) bool {
	for _, s := range states {
		if s != state {
			return false
		}
	}
	return true
}
