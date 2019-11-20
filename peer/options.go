package peer

import (
	"fmt"
	"runtime"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type Options struct {
	Logger logrus.FieldLogger

	Me                 protocol.PeerAddress
	BootstrapAddresses protocol.PeerAddresses
	Codec              protocol.PeerAddressCodec

	// Optional
	DisablePeerDiscovery bool          `json:"disablePeerDiscovery"` // Defaults to false
	Capacity             int           `json:"capacity"`             // capacity of internal channel
	ConnPoolWorkers      int           `json:"connPoolWorkers"`      // Defaults to 2x the number of CPUs
	BootstrapWorkers     int           `json:"bootstrapWorkers"`     // Defaults to 2x the number of CPUs
	BootstrapDuration    time.Duration `json:"bootstrapDuration"`    // Defaults to 1 hour
}

func (options *Options) SetZeroToDefault() error {
	if options.Logger == nil {
		return fmt.Errorf("nil logger")
	}

	if options.Me == nil {
		return fmt.Errorf("nil me address")
	}
	if options.Codec == nil {
		return fmt.Errorf("nil peer address codec")
	}

	if options.Capacity == 0 {
		options.Capacity = 1024
	}
	if options.ConnPoolWorkers == 0 {
		options.ConnPoolWorkers = 2 * runtime.NumCPU()
	}
	if options.BootstrapWorkers <= 0 {
		options.BootstrapWorkers = 2 * runtime.NumCPU()
	}
	if options.BootstrapDuration <= 0 {
		options.BootstrapDuration = time.Hour
	}

	return nil
}
