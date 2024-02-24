package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	pipeline "github.com/roman-baldaev/go-pipeline/pkg"
)

type Sleeper struct {
	Name string
}

func NewSleeper(name string) Sleeper {
	return Sleeper{Name: name}
}

func (s Sleeper) sleep(ctx context.Context) (any, error) {
	start := time.Now()
	cycles := rand.Intn(10)
	for i := 0; i < cycles; i++ {
		select {
		case <-ctx.Done():
			fmt.Println("cancelling", s.Name)
			return nil, nil
		default:
		}
		sl := rand.Intn(5)
		fmt.Println(s.Name, "will sleep for", sl, "seconds...")
		time.Sleep(time.Duration(sl) * time.Second)
		fmt.Println(s.Name, "is awake", "after", sl, "seconds sleep")
	}
	total := time.Since(start)
	return fmt.Sprintf("%s took %v", s.Name, total), nil
}

func cancel(ctx context.Context) (any, error) {
	time.Sleep(1 * time.Second)
	_, cancel := context.WithCancel(ctx)
	cancel()
	return nil, nil
}

func main() {
	concTasks := []pipeline.ConcurrentTask{
		{
			Name: "task1",
			Call: pipeline.ConcurrentCall(NewSleeper("call1").sleep),
		},
		{
			Name: "task2",
			Call: pipeline.ConcurrentCall(NewSleeper("call2").sleep),
		},
		{
			Name: "task3",
			Call: pipeline.ConcurrentCall(NewSleeper("call3").sleep),
		},
	}
	p := pipeline.NewPipeline[int](nil, nil, concTasks)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	res, err := p.Start(ctx)
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
