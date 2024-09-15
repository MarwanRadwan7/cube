package worker

import (
	"fmt"

	"github.com/MarwanRadwan7/cube/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int // Represents the number of tasks a worker has at any given time.
}

// CollectStats used to periodically collect statistics about the worker.
func (w *Worker) CollectStats() {
	fmt.Println("I will collect states")
}

// RunTask responsible for identifying the task’s current state then either starting or stopping a task based on the state.
func (w *Worker) RunTask() {
	fmt.Println("I will start or stop the task based on its state")
}

// StartTask starts a task for the worker.
func (w *Worker) StartTask() {
	fmt.Println("I will start a task")
}

// StopTask stops a task for the worker.
func (w *Worker) StopTask() {
	fmt.Println("I will stop a task")
}
