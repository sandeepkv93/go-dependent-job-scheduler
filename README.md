# Go Dependency Job Scheduler

## Problem statement

Build a Go scheduler that executes dependency-aware jobs in parallel while ensuring that a job starts only after all of its parents complete.

For example:

- Job A and Job B are starting jobs and can run in parallel.
- Job C depends on Job A, and Job D depends on Job B.
- Job E depends on both Job C and Job D.
- Job F, Job G, and Job H depend on Job E and can start in parallel after it completes.
- Job I depends on Job F, Job G, and Job H.

This diagram shows the example flow:

![](flow.png)

## Solution

This repository provides a dependency-aware parallel scheduler implemented in Go. The scheduler:

- validates the input graph before execution
- runs all ready jobs concurrently
- prevents downstream jobs from starting before all parents finish
- stops scheduling new work after the first job failure
- returns explicit errors instead of panicking or hanging on invalid input

## Package overview

- `scheduler`: job model, validation, and execution
- `main.go`: runnable example graph

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/sandeepkv93/go-dependent-job-scheduler/scheduler"
)

func main() {
	jobA, err := scheduler.NewJobWithRunner("A", func(context.Context) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	jobB, err := scheduler.NewJobWithRunner("B", func(context.Context) error {
		return nil
	}, jobA)
	if err != nil {
		log.Fatal(err)
	}

	if err := scheduler.ScheduleAllJobs([]*scheduler.Job{jobA, jobB}); err != nil {
		log.Fatal(err)
	}
}
```

## Validation behavior

- jobs must have non-empty names
- dependencies must be non-nil and unique per job
- the scheduled job list must not contain duplicates
- every dependency must be included in the scheduled job set

## Run

```bash
go run .
```

## Test

```bash
go test ./...
go vet ./...
```
