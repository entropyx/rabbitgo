package rabbitgo

import (
	"fmt"
	"time"
)

type ErrorTimeout struct {
	Timeout time.Duration
	Queue   string
}

func (e ErrorTimeout) Error() string {
	return fmt.Sprintf("%s timeout (%s)", e.Timeout, e.Queue)
}
