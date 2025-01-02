package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/MarwanRadwan7/cube/internal/stats"
	"github.com/MarwanRadwan7/cube/internal/store"
	"github.com/MarwanRadwan7/cube/internal/task"
	"github.com/golang-collections/collections/queue"
)

// TODO: Fix Populating the results from the inspect -- get stat
// TODO: Fix removing the task from the datastore after deleting it

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store
	Stats     *stats.Stats
	TaskCount int // Represents the number of tasks a worker has at any given time.
}

func New(name string, taskDbType string) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}

	// Determine the storage type
	var s store.Store
	var err error
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	case "persistent":
		filename := fmt.Sprintf("%s_tasks.db", name)
		s, err = store.NewTaskStore(filename, 0600, "tasks")
		if err != nil {
			log.Printf("Error adding worker's datastore")
		}
	default:
		s = store.NewInMemoryTaskStore()
	}

	w.Db = s

	// Initialize Stats
	w.Stats = stats.GetStats()
	if w.Stats == nil {
		log.Println("Warning: Initial stats collection failed, will retry in CollectStats routine")
		w.Stats = &stats.Stats{} // Initialize with empty stats to prevent nil pointer
	}
	return &w
}

// Add a new method to safely start worker routines
func (w *Worker) Start() error {
	if w.Db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Start the background routines with some basic error handling
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in RunTasks: %v", r)
			}
		}()
		w.RunTasks()
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in UpdateTasks: %v", r)
			}
		}()
		w.UpdateTasks()
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in CollectStats: %v", r)
			}
		}()
		w.CollectStats()
	}()

	return nil
}

// CollectStats used to periodically collect statistics about the worker.
func (w *Worker) CollectStats() {
	// for {
	// 	log.Println("Collecting stats")
	// 	stats := stats.GetStats()
	// 	if stats == nil {
	// 		log.Println("[Worker] Stats are not available")
	// 		stats = stats.New()
	// 	}
	// 	w.Stats = stats
	// 	time.Sleep(time.Second * 10) // Collect metrics every 10 seconds
	// }
	for {
		log.Println("Collecting stats")
		w.Stats = stats.GetStats()
		w.TaskCount = w.Stats.TaskCount
		time.Sleep(15 * time.Second)
	}
}

// RunTask responsible for identifying the task’s current state then either starting or stopping a task based on the state.
func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("[worker] No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	fmt.Printf("[worker] Found task in queue: %v:\n", taskQueued)

	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("error storing task %s: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}

	result, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("error getting task %s from database: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}

	taskPersisted := *result.(*task.Task)

	if taskPersisted.State == task.Completed {
		return w.StopTask(taskPersisted)
	}

	var dockerResult task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			if taskQueued.ContainerID != "" {
				dockerResult = w.StopTask(taskQueued)
				if dockerResult.Error != nil {
					log.Printf("%v\n", dockerResult.Error)
				}
			}
			dockerResult = w.StartTask(taskQueued)
		default:
			log.Printf("Bad behavior in state transition. taskPersisted: %v, taskQueued: %v\n", taskPersisted, taskQueued)
			dockerResult.Error = fmt.Errorf("bad behavior in state transition")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		dockerResult.Error = err
		return dockerResult
	}
	return dockerResult
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
		w.Db.Put(t.ID.String(), &t)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)

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
	w.Db.Put(t.ID.String(), &t)
	log.Printf("Stopped and removed container %s for task %s\n", t.ContainerID, t.ID)

	return result
}

// AddTask adds a new task to the worker's queue.
func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

// GetTasks retrieves all tasks from the worker's database.
func (w *Worker) GetTasks() []*task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting list of tasks: %v\n", err)
		return nil
	}
	return taskList.([]*task.Task)
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
	d, err := task.NewDocker(config)

	if err != nil {
		log.Printf("Error in inspecting the task: %s , %v\n", t.ID, err)
		return task.DockerInspectResponse{Error: err}
	}

	return d.Inspect(t.ContainerID)
}

// UpdateTasks iterates over the tasks in the worker's database and
// updates their state based on the current status of their associated containers.
func (w *Worker) UpdateTasks() {
	// Check if database is initialized
	if w.Db == nil {
		log.Println("Warning: Database not initialized in worker")
		return
	}

	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting list of tasks: %v\n", err)
		return
	}

	// Check if tasks is nil before type assertion
	if tasks == nil {
		log.Println("No tasks found in database")
		return
	}

	ts, ok := tasks.([]*task.Task)
	if !ok {
		log.Printf("Error: unexpected type from database List() method")
		return
	}

	for _, t := range ts {
		// Skip if task is nil
		if t == nil {
			continue
		}

		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				log.Printf("Error inspecting task %v: %v\n", t.ID, resp.Error)
				continue
			}

			if resp.Container == nil {
				log.Printf("No container for running task: %s\n", t.ID)
				t.State = task.Failed
				err := w.Db.Put(t.ID.String(), t)
				if err != nil {
					log.Printf("Error updating task state: %v\n", err)
				}
				continue
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("Container for task %s is in non-running state: %s", t.ID, resp.Container.State.Status)
				t.State = task.Failed
				err := w.Db.Put(t.ID.String(), t)
				if err != nil {
					log.Printf("Error updating task state: %v\n", err)
				}
				continue
			}

			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			err := w.Db.Put(t.ID.String(), t)
			if err != nil {
				log.Printf("Error updating task ports: %v\n", err)
			}
		}
	}
}
