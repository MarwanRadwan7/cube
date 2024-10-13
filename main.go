package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/MarwanRadwan7/cube/manager"
	"github.com/MarwanRadwan7/cube/task"
	"github.com/MarwanRadwan7/cube/worker"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))
	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting Cube Worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi := worker.Api{
		Address: whost,
		Port:    wport,
		Worker:  &w,
	}

	go runTasks(&w)
	go w.CollectStats()
	go wapi.Start()

	fmt.Println("Starting Cube manager")
	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	m := manager.New(workers)
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}
	go m.ProcessTasks()
	go m.UpdateTasks()
	mapi.Start()

}

func runTasks(w *worker.Worker) {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %s\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		log.Println("Sleeping for 10 seconds.")
		time.Sleep(time.Second * 10)
	}
}
