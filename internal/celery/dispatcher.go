package celery

import (
	"context"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/celery/topology"
)

type TaskHandler interface {
	TaskNames() []string
	Handle(ctx context.Context, taskName string, body []byte) error
}

type QueueBoundHandler interface {
	TaskHandler
	QueueSpecs() []topology.Spec
}

type taskFuncHandler struct {
	taskNames []string
	specs     []topology.Spec
	handle    func(context.Context, string, []byte) error
}

func (h taskFuncHandler) TaskNames() []string {
	return h.taskNames
}

func (h taskFuncHandler) Handle(ctx context.Context, taskName string, body []byte) error {
	return h.handle(ctx, taskName, body)
}

func (h taskFuncHandler) QueueSpecs() []topology.Spec {
	return h.specs
}

func buildTaskHandlerRegistry(handlers ...QueueBoundHandler) (map[string]TaskHandler, error) {
	registry := make(map[string]TaskHandler, len(handlers))
	for _, handler := range handlers {
		for _, name := range handler.TaskNames() {
			if _, exists := registry[name]; exists {
				return nil, fmt.Errorf("duplicate task handler registration for %q", name)
			}
			registry[name] = handler
		}
	}
	return registry, nil
}

func collectQueueSpecs(handlers ...QueueBoundHandler) []topology.Spec {
	var specs []topology.Spec
	for _, handler := range handlers {
		specs = append(specs, handler.QueueSpecs()...)
	}
	return specs
}
