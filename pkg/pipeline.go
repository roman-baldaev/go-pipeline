package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Result struct {
	Name  string
	Error error
	Value any
}

type StartCall[T any] func(context.Context, chan<- T) error

// type SequenceCall[T any] func(context.Context, chan T) chan T
type ConcurrentCall func(context.Context) (any, error)

type SequenceHandler[T any] interface {
	Start(context.Context) error
	Handle(context.Context, T) (T, error)
	Finish(context.Context) error
}

type SequenceTask[T any] struct {
	Name     string
	Capacity uint32
	Call     SequenceHandler[T]
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
	var res <-chan Result
	g, goCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		_res, err := p.startConcurrent(goCtx)
		res = _res
		return err
	})
	g.Go(func() error {
		err := p.startSequence(goCtx)
		return err
	})
	err := g.Wait()
	if err != nil {
		return nil, err
	}
	return res, nil
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

func (p *Pipeline[T]) startSequence(ctx context.Context) error {
	chs := make([]chan T, 0, len(p.SequenceTasks)+1)
	initCh := make(chan T, 10)
	err := p.StartCall(ctx, initCh)
	defer close(initCh)
	chs[0] = initCh
	g, goCtx := errgroup.WithContext(ctx)
	for i, task := range p.SequenceTasks {
		_task := task
		i := i
		ch := make(chan T, task.Capacity)
		chs[i+1] = ch
		_once := sync.Once{}
		fmt.Println("run", _task.Name)
		g.Go(func() error {
			err := p.runSeq(goCtx, _task.Call, &_once, chs[i], chs[i+1])
			fmt.Println("finished", _task.Name)
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (p *Pipeline[T]) runSeq(ctx context.Context, seq SequenceHandler[T], once *sync.Once, in <-chan T, out chan<- T) error {
	defer func() {
		once.Do(func() {
			close(out)
		})
	}()
	err := seq.Start(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case itemIn, ok := <-in:
			if !ok {
				err := seq.Finish(ctx)
				return err
			}
			itemOut, err := seq.Handle(ctx, itemIn)
			if err != nil {
				return err
			}
			out <- itemOut
		case <-ctx.Done():
			err := ctx.Err()
			msg := "sequence stage was cancelled"
			return errors.Wrap(err, msg)
		}
	}
}
