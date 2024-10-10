package main

import (
	"fmt"
	"time"

	"github.com/MarwanRadwan7/cube/task"
	"github.com/MarwanRadwan7/cube/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

// FIXME: Fix naming the containers

func main() {
	db := make(map[uuid.UUID]*task.Task)
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    db,
	}
	t := task.Task{
		ID:    uuid.New(),
		Name:  "test-container-1",
		State: task.Scheduled,
		Image: "strm/helloworld-http",
	}
	// first time the worker will see the task
	fmt.Println("starting task")
	w.AddTask(t)
	result := w.RunTask()
	if result.Error != nil {
		panic(result.Error)
	}

	t.ContainerID = result.ContainerId
	fmt.Printf("task %s is running in container %s\n", t.ID, t.ContainerID)

	fmt.Println("Sleepy time")
	time.Sleep(time.Second * 4)

	fmt.Printf("stopping task %s\n", t.ID)
	t.State = task.Completed
	w.AddTask(t)
	result = w.RunTask()
	if result.Error != nil {
		panic(result.Error)
	}
}
