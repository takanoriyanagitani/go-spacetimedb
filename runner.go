package stdb

import (
	"context"
	"strconv"
	"strings"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

type CommandRunner interface {
	Run([]func() (log []byte, e error)) error
	CreateBuilder(name string, adder s2k.AddBucket) func(context.Context) func() (log []byte, e error)
	UpsertBuilder(name string, setter s2k.Set) func(ctx context.Context, key, val []byte) func() (log []byte, e error)
}

func simpleKv2Cmd(name string, key, val []byte) []byte {
	var vl int = len(val)
	var nk string = strings.Join([]string{
		name,
		string(key),
		strconv.Itoa(vl),
	}, ",") + "\n"
	return append([]byte(nk), val...)
}

type SimpleCommandRunner struct{}

func (s *SimpleCommandRunner) Run(cmds []func() (log []byte, e error)) error {
	for _, c := range cmds {
		_, e := c()
		if nil != e {
			return e
		}
	}
	return nil
}

func (s *SimpleCommandRunner) CreateBuilder(name string, adder s2k.AddBucket) func(context.Context) func() (log []byte, e error) {
	return func(ctx context.Context) func() ([]byte, error) {
		return func() (log []byte, e error) {
			log = []byte(strings.Join([]string{
				"create-bucket",
				name,
			}, ","))
			e = adder(ctx, name)
			return
		}
	}
}

func (s *SimpleCommandRunner) UpsertBuilder(name string, setter s2k.Set) func(ctx context.Context, key, val []byte) func() (log []byte, e error) {
	return func(ctx context.Context, key, val []byte) func() ([]byte, error) {
		return func() (cmd []byte, e error) {
			cmd = simpleKv2Cmd(name, key, val)
			e = setter(ctx, name, key, val)
			return
		}
	}
}

func (s *SimpleCommandRunner) AsRunner() CommandRunner { return s }
