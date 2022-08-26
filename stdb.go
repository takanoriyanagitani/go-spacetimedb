package stdb

import (
	"context"
	"strings"
	"time"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

type Set func(ctx context.Context, id string, date time.Time, key, val []byte) error

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
