package vlcb

import (
	"bytes"
	"testing"
	"time"

	sp "github.com/takanoriyanagitani/go-spacetimedb"
)

func checkerNew[T any](comp func(got T, expected T) bool) func(t *testing.T, got, expected T) {
	return func(t *testing.T, got, expected T) {
		if !comp(got, expected) {
			t.Errorf("Unexpected value got.\n")
			t.Errorf("expected: %v\n", expected)
			t.Errorf("got:      %v\n", got)
		}
	}
}

var checkerBytes = checkerNew(func(a, b []byte) bool { return 0 == bytes.Compare(a, b) })

func checker[T comparable](t *testing.T, got T, expected T) {
	checkerNew(func(a, b T) bool { return a == b })(t, got, expected)
}

func TestVlcb(t *testing.T) {
	t.Parallel()

	t.Run("CborVlogNew", func(t *testing.T) {
		t.Parallel()

		t.Run("Pack/Unpack", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var cv sp.Vlog = CborVlogNew()
				packed, e := cv.Pack(nil)
				if nil != e {
					t.Errorf("Must be nop: %v", e)
				}
				if 0 != len(packed) {
					t.Errorf("Must be empty: %v", len(packed))
				}
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				var cv sp.Vlog = CborVlogNew()
				var dt time.Time = time.Date(
					1970, time.January, 1, 23, 59, 59, 0, time.UTC,
				)
				packed, e := cv.Pack([]sp.TsSample{
					sp.TsSampleNew(
						"idid",
						dt,
						[]byte("k"),
						[]byte("v"),
					),
				})
				if nil != e {
					t.Errorf("Unable to pack: %v", e)
				}
				if 0 == len(packed) {
					t.Errorf("Must not be empty.")
				}

				unpacked, e := cv.Unpack(packed)
				if nil != e {
					t.Errorf("Unable to unpack: %v", e)
				}

				if 1 != len(unpacked) {
					t.Errorf("Unexpected len: %v", len(unpacked))
				}

				var up sp.TsSample = unpacked[0]
				var sdto SampleDto
				up.ForUser(&sdto)

				checker(t, sdto.Id, "idid")
				checker(t, sdto.Date.UnixNano(), dt.UnixNano())
				checkerBytes(t, sdto.Key, []byte("k"))
				checkerBytes(t, sdto.Val, []byte("v"))
			})

			t.Run("multi", func(t *testing.T) {
				t.Parallel()

				var cv sp.Vlog = CborVlogNew()
				var dt time.Time = time.Date(
					1970, time.January, 1, 23, 59, 59, 0, time.UTC,
				)
				packed, e := cv.Pack([]sp.TsSample{
					sp.TsSampleNew(
						"idid",
						dt,
						[]byte("k"),
						[]byte("v"),
					),
					sp.TsSampleNew(
						"iidd",
						dt,
						[]byte("l"),
						[]byte("m"),
					),
				})
				if nil != e {
					t.Errorf("Unable to pack: %v", e)
				}
				if 0 == len(packed) {
					t.Errorf("Must not be empty.")
				}

				unpacked, e := cv.Unpack(packed)
				if nil != e {
					t.Errorf("Unable to unpack: %v", e)
				}

				if 2 != len(unpacked) {
					t.Errorf("Unexpected len: %v", len(unpacked))
				}

				var up sp.TsSample = unpacked[0]
				var sdto SampleDto
				up.ForUser(&sdto)

				checker(t, sdto.Id, "idid")
				checker(t, sdto.Date.UnixNano(), dt.UnixNano())
				checkerBytes(t, sdto.Key, []byte("k"))
				checkerBytes(t, sdto.Val, []byte("v"))

				unpacked[1].ForUser(&sdto)

				checker(t, sdto.Id, "iidd")
				checker(t, sdto.Date.UnixNano(), dt.UnixNano())
				checkerBytes(t, sdto.Key, []byte("l"))
				checkerBytes(t, sdto.Val, []byte("m"))
			})
		})
	})
}
