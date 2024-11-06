package pipeline

import (
	"context"
	"fmt"
	"text/template"
)

type StepConfig struct {
	Stage     *StageConfig
	Sequencer *SequencerConfig
	ChanSize  int
	Workers   int
}

func (conf StepConfig) WithDefaults() StepConfig {
	if conf.Workers <= 0 {
		conf.Workers = 1
	}

	if conf.Sequencer != nil {
		conf.Workers = 1
	}

	if conf.ChanSize <= 0 {
		conf.ChanSize = 1
	}

	return conf
}

func Step(
	ctx context.Context,
	tmpl *template.Template,
	inchan <-chan *Batch,
	outchan chan<- *Batch,
	conf StepConfig,
) error {
	switch {
	case conf.Stage != nil:
		return Stage(ctx, tmpl, inchan, outchan, *conf.Stage)
	case conf.Sequencer != nil:
		return Sequencer(ctx, inchan, outchan, *conf.Sequencer)
	default:
		return fmt.Errorf("step is neither stage nor sequencer")
	}
}
