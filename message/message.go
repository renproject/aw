package message

import (
	"io"

	"github.com/renproject/surge"
)

type Message struct {
	Version uint16
	Variant uint16
	To      []byte
	Data    []byte
}

// Marshal implements the surge.Marshaler interface.
func (m Message) Marshal(w io.Writer) error {
	if err := surge.Marshal(m.Version, w); err != nil {
		return err
	}
	if err := surge.Marshal(m.Variant, w); err != nil {
		return err
	}
	if err := surge.Marshal(m.To, w); err != nil {
		return err
	}
	if err := surge.Marshal(m.Data, w); err != nil {
		return err
	}
	return nil
}

// Unmarshal implements the surge.Unmarshaler interface.
func (m *Message) Unmarshal(r io.Reader) error {
	if err := surge.Unmarshal(&m.Version, r); err != nil {
		return err
	}
	if err := surge.Unmarshal(&m.Variant, r); err != nil {
		return err
	}
	if err := surge.Unmarshal(&m.To, r); err != nil {
		return err
	}
	if err := surge.Unmarshal(&m.Data, r); err != nil {
		return err
	}
	return nil
}

// SizeHint implements the surge.SizeHinter interface.
func (m Message) SizeHint() int {
	return surge.SizeHint(m.Version) + surge.SizeHint(m.Variant) + surge.SizeHint(m.To) + surge.SizeHint(m.Data)
}
