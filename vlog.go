package stdb

type Vlog interface {
	Pack(samples []TsSample) (packed []byte, e error)
	Unpack(packed []byte) (unpacked []TsSample, e error)
}
