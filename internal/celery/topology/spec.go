package topology

// Spec describes the AMQP topology and consumption metadata for one Celery
// task queue.
type Spec struct {
	Exchange     string
	ExchangeType string
	Queue        string
	RoutingKey   string
	ConsumerTag  string
	TaskName     string
}
