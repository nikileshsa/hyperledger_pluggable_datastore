package db

import (
	"fmt"
	"errors"
)

type Constructor func() OpenchainDB

type dbRegistry struct {
	dbs map[string]Constructor
}

var all_registered []string

var Registry = &dbRegistry{
	dbs: make(map[string]Constructor),
}

func (r *dbRegistry) Add(name string, constructor Constructor) (OpenchainDB, error) {
	all_registered = r.Registered()
	if contains(all_registered,name) {
		return nil, errors.New("openchain db: " + name + " is already registered")
	}
	r.dbs[name] = constructor

	return constructor(),nil
}

func (r *dbRegistry) Get(name string) (OpenchainDB, error) {
	constructor, ok := r.dbs[name]
	if !ok {
		return nil, fmt.Errorf("Unregistered db type: %s", name)
	}

	return constructor(), nil
}

func (r *dbRegistry) Registered() []string {
	names := make([]string, len(r.dbs))
	i := 0
	for k := range r.dbs {
		names[i] = k
		i++
	}
	return names
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}


