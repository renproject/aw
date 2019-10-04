package protocol

import "fmt"

type errMessageLengthIsTooLow struct {
	error
	Length MessageLength
}

// NewErrMessageLengthIsTooLow creates a new error which is returned when the
// message length if lower than required.
func NewErrMessageLengthIsTooLow(length MessageLength) error {
	return errMessageLengthIsTooLow{
		error:  fmt.Errorf("message length=%d is too low", length),
		Length: length,
	}
}

type errMessageVersionIsNotSupported struct {
	error
	Version MessageVersion
}

// NewErrMessageVersionIsNotSupported creates a new error which is returned when
// the given message version is not supported.
func NewErrMessageVersionIsNotSupported(version MessageVersion) error {
	return errMessageVersionIsNotSupported{
		error:   fmt.Errorf("message version=%d is not supported", version),
		Version: version,
	}
}

type errMessageVariantIsNotSupported struct {
	error
	Variant MessageVariant
}

// NewErrMessageVariantIsNotSupported creates a new error which is returned when
// the given message variant is not supported.
func NewErrMessageVariantIsNotSupported(variant MessageVariant) error {
	return errMessageVariantIsNotSupported{
		error:   fmt.Errorf("message variant=%d is not supported", variant),
		Variant: variant,
	}
}
