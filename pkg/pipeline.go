package pipeline

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type Result struct {
	Name  string
	Error error
	Value any
}

type StartCall[T any] func(context.Context) chan T
type SequenceCall[T any] func(context.Context, chan T) chan T
type ConcurrentCall func(context.Context) (any, error)

type SequenceTask[T any] struct {
	Name string
	Call SequenceCall[T]
}

type ConcurrentTask struct {
	Name string
	Call ConcurrentCall
}

type Pipeline[T any] struct {
	StartCall       StartCall[T]
	SequenceTasks   []SequenceTask[T]
	ConcurrentTasks []ConcurrentTask
}

func NewPipeline[T any](
	sc StartCall[T], seqt []SequenceTask[T], conct []ConcurrentTask) *Pipeline[T] {
	return &Pipeline[T]{
		StartCall:       sc,
		SequenceTasks:   seqt,
		ConcurrentTasks: conct,
	}
}

func (p *Pipeline[T]) Start(ctx context.Context) (<-chan Result, error) {
	return p.startConcurrent(ctx)
}

func (p *Pipeline[T]) startConcurrent(ctx context.Context) (<-chan Result, error) {
	res := make(chan Result, len(p.ConcurrentTasks))
	defer close(res)
	g, goCtx := errgroup.WithContext(ctx)
	for _, task := range p.ConcurrentTasks {
		_task := task
		fmt.Println("run", _task.Name)
		g.Go(func() error {
			val, err := _task.Call(goCtx)
			_res := Result{
				Name:  _task.Name,
				Error: err,
				Value: val,
			}
			fmt.Println("finished", _task.Name)
			res <- _res
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}
	return res, err
}
