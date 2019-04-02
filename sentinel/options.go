package sentinel

import (
	"github.com/alicebob/miniredis"
	"github.com/mitchellh/copystructure"
)

// Option - how Options are passed as arguments
type Option func(*Options)

// Options = how options are represented
type Options struct {
	master   *miniredis.Miniredis
	replicas []*miniredis.Miniredis
}

// defaultOptions - some defs Options - always deep copy this thing!!!!!
var defaultOptions = Options{
	master:   nil,
	replicas: nil,
}

// WithMaster - set the primary miniredis for the sentinel
func WithMaster(m *miniredis.Miniredis) Option {
	return func(o *Options) {
		o.master = m
	}
}

// WithReplicas - set the replicas for sentinel
func WithReplicas(replicas []*miniredis.Miniredis) Option {
	return func(o *Options) {
		o.replicas = replicas
	}
}

// GetOpts - iterate the inbound Options and return a struct
func GetOpts(opt ...Option) Options {
	dup, _ := copystructure.Copy(defaultOptions)
	opts := dup.(Options)
	for _, o := range opt {
		o(&opts)
	}
	return opts
}
