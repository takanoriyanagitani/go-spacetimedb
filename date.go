package stdb

import (
	"time"
)

const patYmd = "20060102"

type Date2Str func(t time.Time) string

func NewDateConverter(format string) Date2Str {
	return func(t time.Time) string {
		return t.Format(format)
	}
}

var YmdConverter Date2Str = NewDateConverter(patYmd)
