package registry

// lastAccess returns the stored access time of key in Unix nanoseconds.
// key must be present.
func (r *Registry) lastAccess(key string) int64 {
	v, _ := r.m.Load(key)
	return v.(*entry).lastAccess.Load()
}

// index returns the slice index stored on key's entry. key must be present.
func (r *Registry) index(key string) int {
	v, _ := r.m.Load(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	return v.(*entry).index
}
