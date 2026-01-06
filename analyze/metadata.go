package main

type Graph struct {
	Adj [][]int `json:"edges"`
	Deg []int   `json:"degree"`
}

type JobPlan struct {
	OverlayGroup     string  `json:"overlay_group"`
	Protocol         string  `json:"protocol"`
	ExperimanetName  string  `json:"exp_name"`
	NodesCount       int     `json:"nodes_count"`
	AvgDegree        int     `json:"avg_degree"`
	LatencyMS        int     `json:"latency"`
	LossPercentage   int     `json:"loss"`
	Repetitions      int     `json:"repeat"`
	ExpectedValue    float64 `json:"expected_value"`
	StabilizationS   int     `json:"stabilization_wait"`
	EventWaitS       int     `json:"event_wait"`
	EventName        string  `json:"event"`
	AfterEventWaitMS int     `json:"end_wait"`
	EnvFile          string  `json:"params"`
	Graph            Graph   `json:"graph"`
}

type Job struct {
	JobPlan
	ID   int    `json:"id"`
	Host string `json:"host"`
}

type ExperimentRunMetadata struct {
	Job               Job              `json:"job"`
	Repetition        int              `json:"repetition"`
	StartExperimentTs int64            `json:"exp_start_ts"`
	StartEventsTs     int64            `json:"events_start_ts"`
	StopEventsTs      int64            `json:"events_stop_ts"`
	StopExperimentTs  int64            `json:"exp_stop_ts"`
	Events            []*EventMetadata `json:"events"`
}

type EventMetadata struct {
	EventTs       int64    `json:"event_ts"`
	ExpectedValue float64  `json:"expected_value"`
	ExcludeNodes  []string `json:"exclude_nodes"`
}
