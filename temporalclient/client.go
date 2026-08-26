package temporalclient

import (
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func Connect() (client.Client, error) {
	hostPort := os.Getenv("TEMPORAL_HOST_PORT")
	if hostPort == "" {
		hostPort = "localhost:7233"
	}
	return client.Dial(client.Options{HostPort: hostPort})
}

func StartWorker(c client.Client, taskQueue string, register func(w worker.Worker)) (worker.Worker, error) {
	w := worker.New(c, taskQueue, worker.Options{})
	register(w)
	if err := w.Start(); err != nil {
		return nil, err
	}
	return w, nil
}
