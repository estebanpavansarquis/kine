package osclient

func (c *consumer) Revision() (rev int64, err error) {
	c.muRev.RLock()
	defer c.muRev.RUnlock()

	var s Status
	if s, _, err = c.Status(); err != nil {
		return
	}

	rev = s.GetRevision()

	return
}

func (c *consumer) IncrementRevision() (rev int64, err error) {
	c.muRev.Lock()
	defer c.muRev.Unlock()

	var (
		s   Status
		cas string
	)
	if s, cas, err = c.Status(); err != nil {
		return
	}

	rev = s.IncrementRevision()

	if err = c.storeStatus(s, cas, false); err != nil {
		rev = 0
	}

	return
}
