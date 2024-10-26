package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/MarwanRadwan7/cube/stats"
	"github.com/MarwanRadwan7/cube/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	Stats     *stats.Stats
	TaskCount int // Represents the number of tasks a worker has at any given time.
}

// CollectStats used to periodically collect statistics about the worker.
func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")
		w.Stats = stats.GetStats()
		time.Sleep(time.Second * 10) // Collect metrics every 10 seconds
	}
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

// GetTasks retrieves all tasks from the worker's database.
func (w *Worker) GetTasks() []task.Task {
	tasks := make([]task.Task, 0, len(w.Db))
	for _, t := range w.Db {
		tasks = append(tasks, *t)
	}
	return tasks
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()

			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
		}
		log.Println("Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}
}

// InspectTask inspects a given task's Docker container and returns the inspection response.
func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d, _ := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

// UpdateTasks iterates over the tasks in the worker's database and
// updates their state based on the current status of their associated containers.
func (w *Worker) UpdateTasks() {
	for id, t := range w.Db {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				log.Printf("Error in Task: %s\n", id)
			}

			if resp.Container == nil {
				log.Printf("No container for running task: %s\n", id)
				w.Db[id].State = task.Failed
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("Container for task: %s is in non-running state: %s", id, resp.Container.State.Status)
				w.Db[id].State = task.Failed
			}

			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}
	}
}
