package pipeline

import (
	"errors"
	"fmt"
	"sort"
)

// ErrUnknownPipeline is returned when a pipeline+version is not registered.
var ErrUnknownPipeline = errors.New("unknown pipeline")

// Registry holds the known pipeline versions. Populated once at startup, then
// read-only (safe for concurrent workers).
type Registry struct {
	byKey map[string]*Pipeline
}

// NewRegistry constructs an empty registry.
func NewRegistry() *Registry {
	return &Registry{byKey: make(map[string]*Pipeline)}
}

func key(name, version string) string { return name + "@" + version }

// Register adds a pipeline. Panics on a duplicate name+version (a startup-wiring
// bug).
func (r *Registry) Register(p *Pipeline) {
	k := key(p.name, p.version)
	if _, exists := r.byKey[k]; exists {
		panic(fmt.Sprintf("pipeline %q already registered", k))
	}
	r.byKey[k] = p
}

// Get resolves a pipeline by name and version.
func (r *Registry) Get(name, version string) (*Pipeline, error) {
	p, ok := r.byKey[key(name, version)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownPipeline, key(name, version))
	}
	return p, nil
}

// Keys returns the registered pipeline identifiers, sorted (for logging).
func (r *Registry) Keys() []string {
	keys := make([]string, 0, len(r.byKey))
	for k := range r.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
