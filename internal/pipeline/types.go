package pipeline

import (
	"sort"

	"github.com/agnosticeng/agnostic-blockchain-etl/internal/engine"
)

type StartRow struct {
	Start uint64 `ch:"start"`
}

type TipRow struct {
	Tip uint64 `ch:"tip"`
}

type Batch struct {
	Number int
	Conn   engine.Conn
	Start  uint64
	End    uint64
	Vars   map[string]interface{}
}

type BatchBuffer []*Batch

func (bb *BatchBuffer) Insert(b *Batch) {
	var i = sort.Search(len(*bb), func(i int) bool {
		return (*bb)[i].Number > b.Number
	})

	*bb = append(*bb, nil)
	copy((*bb)[i+1:], (*bb)[i:])
	(*bb)[i] = b
}
