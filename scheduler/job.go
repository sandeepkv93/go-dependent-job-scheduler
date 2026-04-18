package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Runner defines the unit of work executed for a job.
type Runner func(context.Context) error

// Job describes a node in the dependency graph.
type Job struct {
	Name string

	runner   Runner
	children []*Job
	parents  []*Job
}

// NewJob creates a job without a custom runner.
func NewJob(name string, parentJobs ...*Job) (*Job, error) {
	return NewJobWithRunner(name, nil, parentJobs...)
}

// NewJobWithRunner creates a job and validates its dependency list.
func NewJobWithRunner(name string, runner Runner, parentJobs ...*Job) (*Job, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("job name must not be empty")
	}

	job := &Job{
		Name:   name,
		runner: runner,
	}

	seenParents := make(map[*Job]struct{}, len(parentJobs))
	for _, parentJob := range parentJobs {
		if parentJob == nil {
			return nil, fmt.Errorf("job %q has a nil dependency", name)
		}
		if _, exists := seenParents[parentJob]; exists {
			return nil, fmt.Errorf("job %q has duplicate dependency %q", name, parentJob.Name)
		}

		seenParents[parentJob] = struct{}{}
		job.parents = append(job.parents, parentJob)
		parentJob.children = append(parentJob.children, job)
	}

	return job, nil
}

// Parents returns the job dependencies in declaration order.
func (job *Job) Parents() []*Job {
	return append([]*Job(nil), job.parents...)
}

// Children returns the dependent jobs in declaration order.
func (job *Job) Children() []*Job {
	return append([]*Job(nil), job.children...)
}

func (job *Job) run(ctx context.Context) error {
	if job.runner == nil {
		return nil
	}

	if err := job.runner(ctx); err != nil {
		return fmt.Errorf("job %q failed: %w", job.Name, err)
	}

	return nil
}
