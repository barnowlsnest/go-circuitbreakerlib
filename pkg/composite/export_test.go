package composite

// cachedOpen returns the cached open-children count without refreshing it.
func (c *Composite) cachedOpen() int {
	open, _ := c.scanned()
	return open
}

// frozen reports whether a trip-time aggregate is held.
func (c *Composite) frozen() bool {
	return c.tripAggregate.Load() != nil
}
