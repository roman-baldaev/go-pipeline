package main

import (
	"context"
	"fmt"

	pipeline "github.com/roman-baldaev/go-pipeline/pkg"
)

type Adder struct {
	IncrementValue int
	IterationCount uint32
}

func NewAdder(iv int) *Adder {
	return &Adder{
		IncrementValue: iv,
	}
}

func (a *Adder) Start(ctx context.Context) error {
	fmt.Printf("start %d adder\n", a.IncrementValue)
	return nil
}

func (a *Adder) Handle(ctx context.Context, val int) (int, error) {
	a.IterationCount++
	new := val + a.IncrementValue
	fmt.Printf("iteration: %d, new value: %d", a.IterationCount, new)
	return new, nil
}

func (a *Adder) Finish(ctx context.Context) error {
	fmt.Printf("finish %d adder\n", a.IncrementValue)
	return nil
}

func main() {
	concTasks := []pipeline.SequenceTask[int]{
		{
			Name: "task1",
			Call: NewAdder(1),
		},
		{
			Name: "task2",
			Call: NewAdder(-1),
		},
		{
			Name: "task3",
			Call: NewAdder(1),
		},
	}
	p := pipeline.NewPipeline[int](nil, nil, concTasks)
	res, err := p.Start(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _res := range res {
		if _res.Error != nil {
			fmt.Println(_res.Error)
			continue
		}
		fmt.Println("task: ", _res.Name, "result: ", _res.Value)
	}
}
