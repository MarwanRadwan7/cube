package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/MarwanRadwan7/cube/task"
	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

type ErrResponse struct {
	HttpStatusCode int
	Message        string
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)

	d.DisallowUnknownFields()
	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling request body: %v\n", err)
		log.Print(msg)
		w.WriteHeader(http.StatusBadRequest)
		e := ErrResponse{
			HttpStatusCode: http.StatusBadRequest,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Worker.StartTask(te.Task)
	log.Printf("Added task %s\n", te.Task.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")

	if taskID == "" {
		log.Printf("No taskID passed in the request\n")
		w.WriteHeader(http.StatusBadRequest)
	}

	tID, _ := uuid.Parse(taskID)

	taskToStop, ok := a.Worker.Db[tID]
	if !ok {
		log.Printf("No task found with ID %s\n", tID)
		w.WriteHeader(http.StatusNotFound)
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %s to stop container %s\n", taskToStop.ID, taskToStop.ContainerID)
	w.WriteHeader(http.StatusNoContent)
}
