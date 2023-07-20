package kafexp

type ErrorHandler interface {
	HandleError(message *RawMessage, err error) error
}

func NewNopErrorHandler() ErrorHandler {
	return nopErrorHandler{}
}

type nopErrorHandler struct{}

func (_ nopErrorHandler) HandleError(_ *RawMessage, _ error) error {
	return nil
}
