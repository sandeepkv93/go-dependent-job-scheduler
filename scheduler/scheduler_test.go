package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestScheduleAllJobsRespectsDependencies(t *testing.T) {
	t.Parallel()

	aDone := make(chan struct{})
	bDone := make(chan struct{})
	cDone := make(chan struct{})
	dDone := make(chan struct{})
	violations := make(chan string, 4)

	var (
		mu       sync.Mutex
		executed []string
	)

	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		executed = append(executed, name)
	}

	jobA, err := NewJobWithRunner("A", func(context.Context) error {
		record("A")
		close(aDone)
		return nil
	})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}

	jobB, err := NewJobWithRunner("B", func(context.Context) error {
		record("B")
		close(bDone)
		return nil
	})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}

	jobC, err := NewJobWithRunner("C", func(context.Context) error {
		select {
		case <-aDone:
		default:
			violations <- "job C started before job A completed"
		}
		record("C")
		close(cDone)
		return nil
	}, jobA)
	if err != nil {
		t.Fatalf("create C: %v", err)
	}

	jobD, err := NewJobWithRunner("D", func(context.Context) error {
		select {
		case <-bDone:
		default:
			violations <- "job D started before job B completed"
		}
		record("D")
		close(dDone)
		return nil
	}, jobB)
	if err != nil {
		t.Fatalf("create D: %v", err)
	}

	jobE, err := NewJobWithRunner("E", func(context.Context) error {
		select {
		case <-cDone:
		default:
			violations <- "job E started before job C completed"
		}
		select {
		case <-dDone:
		default:
			violations <- "job E started before job D completed"
		}
		record("E")
		return nil
	}, jobC, jobD)
	if err != nil {
		t.Fatalf("create E: %v", err)
	}

	if err := ScheduleAllJobs([]*Job{jobA, jobB, jobC, jobD, jobE}); err != nil {
		t.Fatalf("schedule jobs: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(executed) != 5 {
		t.Fatalf("expected 5 executed jobs, got %d", len(executed))
	}
	close(violations)
	for violation := range violations {
		t.Fatal(violation)
	}
}

func TestScheduleAllJobsRequiresFullDependencySet(t *testing.T) {
	t.Parallel()

	parent, err := NewJob("parent")
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	child, err := NewJob("child", parent)
	if err != nil {
		t.Fatalf("create child: %v", err)
	}

	err = ScheduleAllJobs([]*Job{child})
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
}

func TestScheduleAllJobsStopsDownstreamJobsAfterFailure(t *testing.T) {
	t.Parallel()

	rootErr := errors.New("boom")
	root, err := NewJobWithRunner("root", func(context.Context) error {
		return rootErr
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}

	childRan := false
	child, err := NewJobWithRunner("child", func(context.Context) error {
		childRan = true
		return nil
	}, root)
	if err != nil {
		t.Fatalf("create child: %v", err)
	}

	err = ScheduleAllJobs([]*Job{root, child})
	if err == nil {
		t.Fatal("expected scheduler error")
	}
	if !errors.Is(err, rootErr) {
		t.Fatalf("expected root error, got %v", err)
	}
	if childRan {
		t.Fatal("child job should not have run after parent failure")
	}
}

func TestScheduleAllJobsRejectsDuplicateJobs(t *testing.T) {
	t.Parallel()

	job, err := NewJob("A")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	err = ScheduleAllJobs([]*Job{job, job})
	if err == nil {
		t.Fatal("expected duplicate job error")
	}
}

func TestNewJobRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	if _, err := NewJob(""); err == nil {
		t.Fatal("expected empty-name error")
	}

	parent, err := NewJob("parent")
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}

	if _, err := NewJob("child", nil); err == nil {
		t.Fatal("expected nil dependency error")
	}

	if _, err := NewJob("child", parent, parent); err == nil {
		t.Fatal("expected duplicate dependency error")
	}
}
