package worker

import (
	"fmt"
	"log"
	"time"

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
func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No tasks to run in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued, ok := t.(task.Task)
	if !ok {
		log.Println("Error dequeued item is not of type task.Task")
		return task.DockerResult{Error: fmt.Errorf("dequeued item is not of type task.Task")}
	}

	taskPersisted := w.Db[taskQueued.ID]
	// First Time
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = &taskQueued
	}

	// Nth Time
	if !task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		err := fmt.Errorf("invalid transition from %v to %v for task %v", taskPersisted.State, taskQueued.State, taskQueued.ID)
		return task.DockerResult{Error: err}
	}
	var result task.DockerResult
	switch taskQueued.State {
	case task.Scheduled:
		result = w.StartTask(taskQueued)
	case task.Completed:
		result = w.StopTask(taskQueued)
	default:
		log.Printf("Bad behavior in state transition for task %v", taskQueued.ID)
		result.Error = fmt.Errorf("bad behavior in state transition for task %v", taskQueued.ID)
	}

	return result
}

// StartTask starts a task for the worker.
func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d, err := task.NewDocker(config)
	if err != nil {
		log.Printf("Error creating a docker demon for task %s: %v\n", t.ID, err)
		return task.DockerResult{Error: err}
	}
	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %s: %v\n", t.ID, err)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t

	return result
}

// StopTask stops a task for the worker.
func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d, err := task.NewDocker(config)
	if err != nil {
		log.Printf("Error creating a docker demon for task %s: %v\n", t.ID, err)
		return task.DockerResult{Error: err}
	}

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stooping container %s: %v\n", t.ContainerID, result.Error)
		return task.DockerResult{Error: err}
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	log.Printf("Stopped and removed container %s for task %s\n", t.ContainerID, t.ID)

	return result
}

// AddTask adds a new task to the worker's queue.
func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}
