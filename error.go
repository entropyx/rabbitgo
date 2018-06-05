package rabbitgo

import (
	"fmt"
	"time"
)

type ErrorTimeout struct {
	Timeout time.Duration
}

func (e ErrorTimeout) Error() string {
	return fmt.Sprintf("%s timeout", e.Timeout)
}
