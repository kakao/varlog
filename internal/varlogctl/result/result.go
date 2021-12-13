package result

import (
	"errors"

	"go.uber.org/multierr"
)

type Result struct {
	Data  *Data  `json:"data,omitempty"`
	Error *Error `json:"error,omitempty"`
}

func New(kind string) *Result {
	return &Result{
		Data: NewData(kind),
	}
}

func (r *Result) AddErrors(err error) {
	if r.Error == nil {
		r.Error = new(Error)
	}
	r.Data = nil
	r.Error.AddError(err)
}

func (r *Result) AddDataItems(items ...interface{}) {
	if r.Error != nil {
		panic("result having errors")
	}
	for _, item := range items {
		r.Data.AddItem(item)
	}
}

func (r *Result) GetDataItem(idx int) (item interface{}, ok bool) {
	if r.Data != nil && idx < len(r.Data.Items) {
		item = r.Data.Items[idx]
		ok = true
	}
	return
}

func (r *Result) NumberOfDataItem() int {
	if r.Data != nil {
		return len(r.Data.Items)
	}
	return 0
}

func (r *Result) Err() (err error) {
	if r.Error == nil {
		return err
	}
	for i := range r.Error.Errors {
		err = multierr.Append(err, errors.New(r.Error.Errors[i].Message))
	}
	return err
}

type Data struct {
	Kind  string        `json:"kind"`
	Items []interface{} `json:"items"`
}

func NewData(kind string) *Data {
	return &Data{
		Kind:  kind,
		Items: make([]interface{}, 0),
	}
}

func (d *Data) AddItem(item interface{}) {
	d.Items = append(d.Items, item)
}

type Error struct {
	Message string         `json:"message,omitempty"`
	Errors  []*ErrorDetail `json:"errors,omitempty"`
}

func (e *Error) AddError(err error) {
	if len(e.Message) == 0 {
		e.Message = err.Error()
	}
	e.Errors = append(e.Errors, NewErrorDetailWithError(err))
}

type ErrorDetail struct {
	Reason       string `json:"reason,omitempty"`
	Message      string `json:"message,omitempty"`
	ExtendedHelp string `json:"extendedHelper,omitempty"`
}

func NewErrorDetailWithError(err error) *ErrorDetail {
	return &ErrorDetail{
		Reason:  err.Error(),
		Message: err.Error(),
	}
}
