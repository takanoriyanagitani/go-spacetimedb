package stdb

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
	spx "github.com/takanoriyanagitani/go-sql2keyval/pkg/postgres/pgx"
)

func BenchmarkStdb(b *testing.B) {
	pgx_dbname := os.Getenv("ITEST_SPACETIMEDB_PGX_DBNAME")
	if len(pgx_dbname) < 1 {
		b.Skip("skipping pgx benchmark...")
	}

	pool, e := pgxpool.Connect(context.Background(), "dbname="+pgx_dbname)
	if nil != e {
		b.Errorf("Unable to get pgx pool: %v", e)
	}

	b.Run("pgx benchmark", func(b *testing.B) {

		var tableDropper s2k.DelBucket = spx.PgxDelBucketNew(pool)

		var tableCreater s2k.AddBucket = spx.PgxAddBucketNew(pool)
		var upsert s2k.SetBatch = spx.PgxBatchUpsertNew(pool)
		var fastCreator s2k.AddBucket = FastBucketAdderNew(tableCreater)
		var d2s Date2Str = func(_ time.Time) string { return "2022_09_01" }

		tables := []string{
			"dates",
			"dates_idid",
			"devices",
			"devices_2022_09_01",
			"data_2022_09_01_idid",
		}

		for _, tablename := range tables {
			e := tableDropper(context.Background(), tablename)
			if nil != e {
				b.Errorf("Unable to drop table: %v", e)
			}
		}

		var bsBuilder func(lmt int) BatchSet = func(lmt int) BatchSet {
			return NewBatchSetter(fastCreator, upsert, lmt)(d2s)
		}

		var batchSetter8 BatchSet = bsBuilder(1048576)

		randBytes := make([]byte, 8192)
		updtRand := func() { _, _ = rand.Read(randBytes) }
		_, e := rand.Read(randBytes)
		if nil != e {
			b.Errorf("Unable to init rand bytes: %v", e)
		}

		i2sample := func(_ int) TsSample {
		    keybuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(keybuf, uint64(time.Now().UnixNano()))
			updtRand()
			return TsSampleNew("idid", time.Now(), keybuf, randBytes)
		}

		b.Run("BatchSet", func(b *testing.B) {

			b.Run("iter sizes", func(b *testing.B) {
				itersizes := []int{0, 3, 16, 128, 256}

				for _, itersz := range itersizes {
					b.Run("iter sz: "+strconv.Itoa(itersz), func(b *testing.B) {
						b.ResetTimer()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								var integers s2k.Iter[int] = s2k.IterInts(0, itersz)
								var samples s2k.Iter[TsSample] = s2k.IterMap(integers, i2sample)
								e := batchSetter8(context.Background(), samples)
								if nil != e {
									b.Errorf("Unable to upsert: %v", e)
								}
							}
						})
						b.ReportMetric(float64(b.N)*float64(itersz), "inserts")
					})
				}
			})

		})

	})

	b.Cleanup(func() { pool.Close() })
}
