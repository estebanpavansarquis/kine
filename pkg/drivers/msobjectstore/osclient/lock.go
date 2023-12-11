package osclient

/*
import (
	"time"
)

func (c *consumer) Lock() (success bool, lockingKey string, err error) {
	var status Status
	if status, err = c.Status(); err != nil {
		return
	}

	var ok bool
	for {
		if ok, lockingKey = status.Lock(); !ok {
			time.Sleep(50 * time.Millisecond)
		} else {
			break
		}
	}

	return
}

func (c *consumer) Unlock() (success bool, err error) {

	return
}
*/
