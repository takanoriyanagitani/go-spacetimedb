package stdb

import (
	"context"
	"testing"
	"time"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func comp[T comparable](expected, got T, t *testing.T) {
	if expected != got {
		t.Errorf("Unexpected value got.\n")
		t.Errorf("Expected: %v\n", expected)
		t.Errorf("Got: %v\n", got)
	}
}

func TestNewStName(t *testing.T) {
	t.Parallel()
	expected := "data_20220826_cafef00ddeadbeafface864299792458"
	got := newStName("cafef00ddeadbeafface864299792458", "20220826")
	comp(expected, got, t)
}

func TestNewDatesName(t *testing.T) {
	t.Parallel()
	expected := "dates_cafef00ddeadbeafface864299792458"
	got := newDatesName("cafef00ddeadbeafface864299792458")
	comp(expected, got, t)
}

func TestNewDevicesName(t *testing.T) {
	t.Parallel()
	expected := "devices_20220826"
	got := newDevicesName("20220826")
	comp(expected, got, t)
}

func TestNewSetter(t *testing.T) {
	t.Parallel()

	var dummyAdder s2k.AddBucket = func(_c context.Context, _b string) error { return nil }
	var dummySetter s2k.Set = func(_c context.Context, _b string, _k []byte, _v []byte) error { return nil }

	var setterBuilder func(dateConverter Date2Str, runner CommandRunner) Set = NewSetter(dummyAdder, dummySetter)
	if nil == setterBuilder {
		t.Errorf("Unable to get setter builder")
	}

	var dtconv Date2Str = YmdConverter
	var runner CommandRunner = &SimpleCommandRunner{}

	var setter Set = setterBuilder(dtconv, runner)
	if nil == setter {
		t.Errorf("Unable to get setter")
	}

	tm := time.Date(1970, time.January, 1, 23, 59, 59, 0, time.UTC)
	e := setter(context.Background(), "cafef00ddeadbeafface864299792458", tm, []byte("hw"), []byte("vl"))
	if nil != e {
		t.Errorf("Unable to set: %v", e)
	}
}
