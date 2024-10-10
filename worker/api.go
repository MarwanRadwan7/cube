package worker

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
)

type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Get("/", a.GetTasksHandler)
		r.Post("/", a.StartTaskHandler)

		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
}

// Start initializes the router and starts the HTTP server that wraps the Worker to serve Manager's Requests.
func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}
