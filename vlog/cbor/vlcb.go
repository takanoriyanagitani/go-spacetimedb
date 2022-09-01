package vlcb

import (
	"bytes"
	"time"

	"github.com/fxamacker/cbor/v2"

	sp "github.com/takanoriyanagitani/go-spacetimedb"
)

type CborPack func(samples []sp.TsSample) (packed []byte, e error)
type CborUnpack func(packed []byte) (unpacked []sp.TsSample, e error)

type cborVlog struct {
	packer   CborPack
	unpacker CborUnpack
}

func CborVlogNew() *cborVlog {
	return &cborVlog{
		packer:   newPacker(),
		unpacker: newUnpacker(),
	}
}

func (c *cborVlog) Pack(samples []sp.TsSample) ([]byte, error)  { return c.packer(samples) }
func (c *cborVlog) Unpack(packed []byte) ([]sp.TsSample, error) { return c.unpacker(packed) }

type SampleDto struct {
	Id   string
	Date time.Time
	Key  []byte
	Val  []byte
}

func (s *SampleDto) UseId(id string)      { s.Id = id }
func (s *SampleDto) UseDate(dt time.Time) { s.Date = dt }
func (s *SampleDto) UseKey(k []byte)      { s.Key = k }
func (s *SampleDto) UseVal(v []byte)      { s.Val = v }

func (s *SampleDto) ToBytes(buf *bytes.Buffer) (packed []byte, e error) {
	encoder := cbor.NewEncoder(buf)
	e = encoder.Encode(s)
	return buf.Bytes(), e
}

func FromBytes(b []byte) (unpacked SampleDto, e error) {
	rdr := bytes.NewReader(b)
	decoder := cbor.NewDecoder(rdr)
	e = decoder.Decode(&unpacked)
	return
}

func newPacker() CborPack {
	var buf bytes.Buffer
	return func(samples []sp.TsSample) (packed []byte, e error) {
		for _, ts := range samples {
			var s SampleDto
			ts.ForUser(&s)
			_, e = s.ToBytes(&buf)
			if nil != e {
				return nil, e
			}
		}
		return buf.Bytes(), nil
	}
}

func newUnpacker() CborUnpack {
	return func(packed []byte) (unpacked []sp.TsSample, e error) {
		return
	}
}
