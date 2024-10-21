package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

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
	wapi := worker.Api{Address: whost, Port: wport, Worker: &w}

	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()
	go wapi.Start()

	fmt.Println("Starting Cube Manager")
	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}

	m := manager.New(workers)
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	mapi.Start()
}
