package bills

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"encore.app/temporalclient"
)

//encore:service
type Service struct {
	temporal client.Client
	worker   worker.Worker
}

func initService() (*Service, error) {
	c, err := temporalclient.Connect()
	if err != nil {
		return nil, err
	}

	w, err := temporalclient.StartWorker(c, TaskQueue, func(w worker.Worker) {
		w.RegisterWorkflow(BillWorkflow)
		w.RegisterActivity(RecordLineItemActivity)
		w.RegisterActivity(RecordBillClosedActivity)
	})
	if err != nil {
		c.Close()
		return nil, err
	}

	return &Service{temporal: c, worker: w}, nil
}

func (s *Service) Shutdown(force context.Context) {
	s.worker.Stop()
	s.temporal.Close()
}
