package stdb

import (
	"context"
	"strings"
	"sync"
	"time"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

type Set func(ctx context.Context, id string, date time.Time, key, val []byte) error

type TsSample struct {
	id   string
	date time.Time
	pair s2k.Pair
}

func TsSampleNew(id string, date time.Time, Key, Val []byte) TsSample {
	pair := s2k.Pair{Key, Val}
	return TsSample{
		id,
		date,
		pair,
	}
}

func (t TsSample) ToDatesTableName() string               { return "dates_" + t.id }              // dates_cafef00ddeadbeafface864299792458
func (t TsSample) ToDevicesTableName(dtymd string) string { return "devices_" + dtymd }           // devices_2022_08_31
func (t TsSample) ToDtDvTableName(dtymd string) string    { return "data_" + dtymd + "_" + t.id } // data_2022_08_31_cafef00ddeadbeafface864299792458
func (t TsSample) AsKey() []byte                          { return t.pair.Key }
func (t TsSample) AsVal() []byte                          { return t.pair.Val }

func (t TsSample) ToBatch(d2s Date2Str) s2k.Iter[s2k.Batch] {
	bid := []byte(t.id)
	ymd := d2s(t.date)
	bym := []byte(ymd)
	emp := []byte("")
	return s2k.IterFromArray([]s2k.Batch{
		s2k.BatchNew("devices", bid, emp),
		s2k.BatchNew("dates", bym, emp),
		s2k.BatchNew(t.ToDatesTableName(), bym, emp),
		s2k.BatchNew(t.ToDevicesTableName(ymd), bid, emp),
		s2k.BatchNew(t.ToDtDvTableName(ymd), t.AsKey(), t.AsVal()),
	})
}

type BatchSet func(ctx context.Context, b s2k.Iter[TsSample]) error

func newStName(id string, ymd string) string {
	return strings.Join([]string{
		"data",
		ymd,
		id,
	}, "_")
}

func newDatesName(id string) string {
	return strings.Join([]string{
		"dates",
		id,
	}, "_")
}

func newDevicesName(ymd string) string {
	return strings.Join([]string{
		"devices",
		ymd,
	}, "_")
}

func NewSetter(adder s2k.AddBucket, setter s2k.Set) func(dateConverter Date2Str, runner CommandRunner) Set {
	return func(dateConverter Date2Str, cmdRunner CommandRunner) Set {
		return func(ctx context.Context, id string, date time.Time, key, val []byte) error {
			var dtymd string = dateConverter(date)
			var dt_dev string = newDatesName(id)      // dates_cafef00ddeadbeafface864299792458
			var dvdate string = newDevicesName(dtymd) // devices_20220826
			var stname string = newStName(id, dtymd)  // data_20220826_cafef00ddeadbeafface864299792458
			var devid []byte = []byte(id)
			var dbyte []byte = []byte(dtymd)
			return cmdRunner.Run([]func() ([]byte, error){
				// create buckets
				cmdRunner.CreateBuilder("devices", adder)(ctx),
				cmdRunner.CreateBuilder("dates", adder)(ctx),
				cmdRunner.CreateBuilder(dt_dev, adder)(ctx),
				cmdRunner.CreateBuilder(dvdate, adder)(ctx),
				cmdRunner.CreateBuilder(stname, adder)(ctx),

				// upserts
				cmdRunner.UpsertBuilder("devices", setter)(ctx, devid, []byte("")),
				cmdRunner.UpsertBuilder("dates", setter)(ctx, dbyte, []byte("")),
				cmdRunner.UpsertBuilder(dt_dev, setter)(ctx, dbyte, []byte("")), // dates_cafef00ddeadbeafface864299792458
				cmdRunner.UpsertBuilder(dvdate, setter)(ctx, devid, []byte("")), // devices_20220826
				cmdRunner.UpsertBuilder(stname, setter)(ctx, key, val),
			})
		}
	}
}

func FastBucketAdderNew(adder s2k.AddBucket) s2k.AddBucket {
	mp := make(map[string]interface{})
	var lk sync.RWMutex

	hasBucket := func(b string) bool {
		lk.RLock()
		_, ok := mp[b]
		lk.RUnlock()
		return ok
	}

	addBucket := func(ctx context.Context, b string) error {
		e := adder(ctx, b)
		if nil != e {
			return e
		}
		lk.Lock()
		defer lk.Unlock()
		mp[b] = nil
		return e
	}

	return func(ctx context.Context, bucket string) error {
		if hasBucket(bucket) {
			return nil
		}
		return addBucket(ctx, bucket)
	}
}

func samples2batch(s s2k.Iter[TsSample], d2s Date2Str, lmt int) s2k.Iter[s2k.Batch] {
	mapd := s2k.IterMap(s, func(ts TsSample) s2k.Iter[s2k.Batch] { return ts.ToBatch(d2s) })
	c := make(chan s2k.Batch, lmt)
	s2k.IterFlat2Chan(mapd, c, lmt)
	close(c)
	return s2k.IterFromChan(c)
}

func NewBatchSetter(fastAdder s2k.AddBucket, setter s2k.SetBatch, lmt int) func(dateConverter Date2Str) BatchSet {
	return func(dateConverter Date2Str) BatchSet {
		return func(ctx context.Context, many s2k.Iter[TsSample]) error {
			var bi s2k.Iter[s2k.Batch] = samples2batch(many, dateConverter, lmt)
			var createOrIgnore = bi.IntoInspect(func(b s2k.Batch) {
				_ = fastAdder(ctx, b.Bucket()) // unable to create missing table -> query will be rejected anyway
			})
			return setter(ctx, createOrIgnore)
		}
	}
}
