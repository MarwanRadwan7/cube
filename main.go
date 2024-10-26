package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/MarwanRadwan7/cube/manager"
	"github.com/MarwanRadwan7/cube/worker"
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

	// Initially, Start 3 Workers
	w1 := worker.New("worker-1", "memory")
	w2 := worker.New("worker-2", "memory")
	w3 := worker.New("worker-3", "memory")
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	// Worker 1
	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	// Worker 2
	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	// Worker 3
	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	fmt.Println("Starting Cube Manager")
	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}

	//m := manager.New(workers, "roundrobin")
	m := manager.New(workers, "epvm", "memory")
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}
