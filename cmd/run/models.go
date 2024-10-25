package run

import "github.com/agnosticeng/agnostic-blockchain-etl/internal/ch"

type startRow struct {
	Start uint64 `ch:"start"`
}

type maxEndRow struct {
	MaxEnd uint64 `ch:"max_end"`
}

type batch struct {
	Conn  *ch.Conn
	Start uint64
	End   uint64
	Vars  map[string]interface{}
}
