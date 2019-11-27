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
	NumWorkers           int           `json:"numWorkers"`           // Defaults to 2x the number of CPUs
	Alpha                int           `json:"alpha"`                // Defaults to 2x the number of BootstrapAddress
	BootstrapDuration    time.Duration `json:"bootstrapDuration"`    // Defaults to 1 hour
	MinPingTimeout       time.Duration `json:"minPingTimeout"`       // Defaults to 1 second
	MaxPingTimeout       time.Duration `json:"maxPingTimeout"`       // Defaults to 30 seconds
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
	if options.NumWorkers == 0 {
		options.NumWorkers = 2 * runtime.NumCPU()
	}
	// TODO : improve the default value of alpha
	if options.Alpha == 0 {
		if len(options.BootstrapAddresses) != 0 {
			options.Alpha = 2 * len(options.BootstrapAddresses)
		} else {
			options.Alpha = 10
		}
	}
	if options.BootstrapDuration <= 0 {
		options.BootstrapDuration = time.Hour
	}
	if options.MinPingTimeout <= 0 {
		options.MinPingTimeout = time.Second
	}
	if options.MaxPingTimeout <= 0 {
		options.MaxPingTimeout = 30 * time.Second
	}

	return nil
}
