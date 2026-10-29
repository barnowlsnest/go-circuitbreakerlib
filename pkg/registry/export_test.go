package registry

// lastAccess returns the stored access time of key in nanoseconds since the
// registry was created.
// key must be present.
func (r *Registry) lastAccess(key string) int64 {
	e, _ := r.load(key)
	return e.lastAccess.Load()
}

// index returns the slice index stored on key's entry. key must be present.
func (r *Registry) index(key string) int {
	e, _ := r.load(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	return e.index
}
