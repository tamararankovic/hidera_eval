package main

var eventFns = map[string]func(Job) []EventMetadata{
	"noop": NoopEvent,
}

func NoopEvent(job Job) []EventMetadata {
	return []EventMetadata{}
}
