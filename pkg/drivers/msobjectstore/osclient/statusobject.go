package osclient

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

type Status interface {
	GetRevision() int64
	IncrementRevision() int64
	//Lock() (success bool, lockID string)
	//Unlock(lockID string) (success bool)
	Encode() (string, error)
	String() string
}

type statusObject struct {
	Revision int64 `json:"revision"`
	//locked   bool         `json:"locked"`
	//lockKey  string       `json:"lockKey"`
	Created string `json:"created"`
	Updated string `json:"updated"`
}

func NewStatusObject() Status {
	now := time.Now().String()
	return &statusObject{
		Created: now,
		Updated: now,
	}
}

func NewStatusObjectFrom(bin string) (Status, error) {
	var st statusObject
	if e := json.Unmarshal([]byte(bin), &st); e != nil {
		logrus.Errorf("error while unmarshalling status object binary value: %s => %s", bin, e.Error())
		return nil, ErrUnarshallFailed
	}
	return &st, nil
}

func (s *statusObject) String() string {
	//return fmt.Sprintf("revision: %d, locked: %t, created: %s, updated: %s", s.GetRevision s.locked, s.Created, s.Updated)
	return fmt.Sprintf("revision: %d, created: %s, updated: %s", s.GetRevision(), s.Created, s.Updated)
}

func (s *statusObject) Encode() (data string, err error) {
	var v []byte
	if v, err = json.Marshal(s); err != nil {
		err = ErrMarshallFailed
		return
	}

	data = string(v)

	return
}

func (s *statusObject) GetRevision() int64 {
	return s.Revision
}

func (s *statusObject) IncrementRevision() int64 {
	s.Revision++
	s.Updated = time.Now().String()
	return s.Revision
}

/*
func (s *statusObject) Lock() (success bool, lockID string) {
	s.mut.RLock()
	defer s.mut.Unlock()

	if s.locked {
		return
	}

	s.locked = true
	lockID = uuid.NewString()
	s.updated = time.Now().String()
	success = true
	return
}

func (s *statusObject) Unlock(lockID string) (success bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if !s.locked {
		return
	}

	if s.lockKey != lockID {
		return
	}

	s.locked = false
	s.lockKey = ""
	s.updated = time.Now().String()
	success = true

	return
}
*/
