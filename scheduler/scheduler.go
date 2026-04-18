package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ScheduleAllJobs runs the provided jobs once their dependencies are satisfied.
func ScheduleAllJobs(jobs []*Job) error {
	return Run(context.Background(), jobs)
}

// Run executes a dependency graph until completion or the first failure.
func Run(ctx context.Context, jobs []*Job) error {
	normalizedJobs, err := validateJobs(jobs)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remainingParents := make(map[*Job]int, len(normalizedJobs))
	readyJobs := make([]*Job, 0, len(normalizedJobs))
	for _, job := range normalizedJobs {
		remaining := len(job.parents)
		remainingParents[job] = remaining
		if remaining == 0 {
			readyJobs = append(readyJobs, job)
		}
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		firstErr  error
		recordErr sync.Once
	)

	var launch func(*Job)
	launch = func(job *Job) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := job.run(ctx); err != nil {
				recordErr.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}

			var readyChildren []*Job

			mu.Lock()
			for _, child := range job.children {
				remaining, tracked := remainingParents[child]
				if !tracked {
					continue
				}

				remaining--
				remainingParents[child] = remaining
				if remaining == 0 {
					readyChildren = append(readyChildren, child)
				}
			}
			mu.Unlock()

			if ctx.Err() != nil {
				return
			}

			for _, child := range readyChildren {
				launch(child)
			}
		}()
	}

	for _, job := range readyJobs {
		launch(job)
	}

	wg.Wait()

	return firstErr
}

func validateJobs(jobs []*Job) ([]*Job, error) {
	if len(jobs) == 0 {
		return nil, errors.New("no jobs provided")
	}

	normalizedJobs := make([]*Job, 0, len(jobs))
	seen := make(map[*Job]struct{}, len(jobs))

	for index, job := range jobs {
		if job == nil {
			return nil, fmt.Errorf("job at index %d is nil", index)
		}
		if _, exists := seen[job]; exists {
			return nil, fmt.Errorf("job %q was provided more than once", job.Name)
		}

		seen[job] = struct{}{}
		normalizedJobs = append(normalizedJobs, job)
	}

	for _, job := range normalizedJobs {
		for _, parent := range job.parents {
			if _, exists := seen[parent]; !exists {
				return nil, fmt.Errorf("job %q depends on %q which was not provided", job.Name, parent.Name)
			}
		}
	}

	return normalizedJobs, nil
}
