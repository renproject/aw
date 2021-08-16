package wire

// NegligibleError are errors can be ignored by the logger.
type NegligibleError struct {
	Err error
}

func (ne NegligibleError) Error() string {
	return ne.Err.Error()
}

func NewNegligibleError(err error) error {
	return NegligibleError{Err: err}
}
