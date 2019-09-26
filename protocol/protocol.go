package protocol

import (
	"context"

	"github.com/renproject/phi"
)

type Runner interface {
	Run(ctx context.Context)
}

type Runners []Runner

func (runners Runners) Run(ctx context.Context) {
	phi.ParForAll(runners, func(i int) {
		runners[i].Run(ctx)
	})
}

func NewRunners(runners ...Runner) Runner {
	return Runners(runners)
}
