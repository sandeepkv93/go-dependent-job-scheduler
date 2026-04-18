package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sandeepkv93/go-dependent-job-scheduler/scheduler"
)

func main() {
	jobA := mustJob("Job A", 150*time.Millisecond)
	jobB := mustJob("Job B", 200*time.Millisecond)
	jobC := mustJob("Job C", 120*time.Millisecond, jobA)
	jobD := mustJob("Job D", 120*time.Millisecond, jobB)
	jobE := mustJob("Job E", 100*time.Millisecond, jobC, jobD)
	jobF := mustJob("Job F", 90*time.Millisecond, jobE)
	jobG := mustJob("Job G", 90*time.Millisecond, jobE)
	jobH := mustJob("Job H", 90*time.Millisecond, jobE)
	jobI := mustJob("Job I", 75*time.Millisecond, jobF, jobG, jobH)

	allJobs := []*scheduler.Job{jobA, jobB, jobC, jobD, jobE, jobF, jobG, jobH, jobI}
	if err := scheduler.ScheduleAllJobs(allJobs); err != nil {
		log.Fatal(err)
	}
}

func mustJob(name string, duration time.Duration, parents ...*scheduler.Job) *scheduler.Job {
	job, err := scheduler.NewJobWithRunner(name, exampleRunner(name, duration), parents...)
	if err != nil {
		log.Fatalf("create %s: %v", name, err)
	}

	return job
}

func exampleRunner(name string, duration time.Duration) scheduler.Runner {
	return func(ctx context.Context) error {
		fmt.Printf("%s started\n", name)

		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		fmt.Printf("%s completed\n", name)
		return nil
	}
}
