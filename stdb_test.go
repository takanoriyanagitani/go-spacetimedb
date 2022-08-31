package stdb

import (
	"context"
	"testing"
	"time"

	// it only
	"database/sql"
	"fmt"
	"os"

	// use pgx driver to connect to postgres
	_ "github.com/jackc/pgx/v4/stdlib"

	// use postgres driver to generate sql
	_ "github.com/takanoriyanagitani/go-sql2keyval/pkg/sqldb/postgres"

	// use pgx(direct) to connect to postgres
	"github.com/jackc/pgx/v4/pgxpool"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
	spx "github.com/takanoriyanagitani/go-sql2keyval/pkg/postgres/pgx"
	std "github.com/takanoriyanagitani/go-sql2keyval/pkg/stdsql"
)

var (
	adderPg  s2k.AddBucket
	setterPg s2k.Set
	testDb   *sql.DB
)

func init() {
	dbname, e := itGetPgxEnvDb()
	if nil != e {
		// unit test only
		// no need to set up adder/setter
		return
	}

	dbnew := std.DbOpenNew("pgx")
	conn := "dbname=" + dbname
	testDb, e = dbnew(conn)
	if nil != e {
		panic(e)
	}

	var exec s2k.Exec = std.ExecNew(testDb)

	adderPg = s2k.AddBucketFactory("postgres")(exec)
	setterPg = s2k.SetFactory("postgres")(exec)
}

func itGetPgxEnvDb() (dbname string, e error) {
	dbname = os.Getenv("ITEST_SPACETIMEDB_PGX_DBNAME")
	if 0 < len(dbname) {
		return
	}
	return "", fmt.Errorf("skipping pgx test")
}

func comp[T comparable](expected, got T, t *testing.T) {
	if expected != got {
		t.Errorf("Unexpected value got.\n")
		t.Errorf("Expected: %v\n", expected)
		t.Errorf("Got: %v\n", got)
	}
}

func TestPgx(t *testing.T) {
	if nil == testDb {
		t.Skip("skipping pgx test")
	}

	t.Parallel()

	t.Run("get setter factory", func(t *testing.T) {
		t.Parallel()
		setterBuilder := NewSetter(adderPg, setterPg)
		if nil == setterBuilder {
			t.Errorf("Unable to get setter factory")
		}
	})

	t.Run("get setter", func(t *testing.T) {
		t.Parallel()
		setterBuilder := NewSetter(adderPg, setterPg)
		if nil == setterBuilder {
			t.Errorf("Unable to get setter factory")
		}

		var dtconv Date2Str = YmdConverter
		var runner CommandRunner = &SimpleCommandRunner{}
		var setter Set = setterBuilder(dtconv, runner)

		if nil == setter {
			t.Errorf("Unable to get setter")
		}
	})

	t.Run("setter", func(t *testing.T) {
		t.Parallel()

		setterBuilder := NewSetter(adderPg, setterPg)
		if nil == setterBuilder {
			t.Errorf("Unable to get setter factory")
		}

		var dtconv Date2Str = YmdConverter
		var runner CommandRunner = &SimpleCommandRunner{}
		var setter Set = setterBuilder(dtconv, runner)

		if nil == setter {
			t.Errorf("Unable to get setter")
		}

		t.Run("epoch", func(t *testing.T) {
			t.Parallel()

			e := setter(
				context.Background(),
				"cafef00ddeadbeafface864299792458",
				time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
				[]byte("hw"),
				[]byte("42"),
			)

			if nil != e {
				t.Errorf("Unable to upsert key/val: %v", e)
			}
		})
	})

	t.Cleanup(func() { testDb.Close() })
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

func checker[T comparable](expected, got T, t *testing.T) {
	if expected != got {
		t.Errorf("Unexpected value got.")
		t.Errorf("expected: %v", expected)
		t.Errorf("got: %v", got)
	}
}

func TestStdb(t *testing.T) {
	t.Parallel()

	t.Run("ToBatch", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			ts := TsSample{
				id:   "iidd",
				date: time.Now(),
				pair: s2k.Pair{
					Key: nil,
					Val: nil,
				},
			}
			var d2s Date2Str = func(_ time.Time) string { return "1970_01_01" }
			var b s2k.Iter[s2k.Batch] = ts.ToBatch(d2s)

			checker("devices", b().Value().Bucket(), t)
			checker("dates", b().Value().Bucket(), t)
			checker("dates_iidd", b().Value().Bucket(), t)
			checker("devices_1970_01_01", b().Value().Bucket(), t)
			checker("data_1970_01_01_iidd", b().Value().Bucket(), t)

			checker(true, b().Empty(), t)
		})
	})

	t.Run("pgx test", func(t *testing.T) {
		t.Parallel()

		pgx_dbname := os.Getenv("ITEST_SPACETIMEDB_PGX_DBNAME")
		if len(pgx_dbname) < 1 {
			t.Skip("skipping pgx test...")
		}

		pool, e := pgxpool.Connect(context.Background(), "dbname="+pgx_dbname)
		if nil != e {
			t.Errorf("Unable to connect: %v", e)
		}

		t.Run("pgx con got", func(t *testing.T) {
			t.Parallel()

			var tableCreater s2k.AddBucket = spx.PgxAddBucketNew(pool)
			var upsert s2k.SetBatch = spx.PgxBatchUpsertNew(pool)

			var d2s Date2Str = func(_ time.Time) string { return "1980_01_01" }

			var batchSetBuilder func(Date2Str) BatchSet = NewBatchSetter(tableCreater, upsert, 16)
			var batchSetter BatchSet = batchSetBuilder(d2s)

			t.Run("empty", func(t *testing.T) {
				t.Parallel()
				var samples s2k.Iter[TsSample] = s2k.IterFromArray([]TsSample{})

				e := batchSetter(context.Background(), samples)
				if nil != e {
					t.Errorf("Unable to upsert key/val: %v", e)
				}
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()
				var samples s2k.Iter[TsSample] = s2k.IterFromArray([]TsSample{
					TsSampleNew("i3776", time.Now(), []byte("k"), []byte("v")),
				})

				e := batchSetter(context.Background(), samples)
				if nil != e {
					t.Errorf("Unable to upsert key/val: %v", e)
				}
			})

			t.Run("multi", func(t *testing.T) {
				t.Parallel()
				var samples s2k.Iter[TsSample] = s2k.IterFromArray([]TsSample{
					TsSampleNew("i634", time.Now(), []byte("k"), []byte("v")),
					TsSampleNew("i333", time.Now(), []byte("l"), []byte("w")),
				})

				e := batchSetter(context.Background(), samples)
				if nil != e {
					t.Errorf("Unable to upsert key/val: %v", e)
				}
			})
		})

		t.Cleanup(func() {
			pool.Close()
		})
	})
}
