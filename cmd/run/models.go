package run

type startRow struct {
	Start uint64 `ch:"start"`
}

type maxEndRow struct {
	MaxEnd uint64 `ch:"max_end"`
}

type batch struct {
	Conn  *Conn
	Start uint64
	End   uint64
	Vars  map[string]interface{}
}
