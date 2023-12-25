// Provides facility to read request data from net/http request into a DataForm instance
//
// Note that validation is not done on the data.
package nethttp

import (
	"SharkFourSix/crudiator"
	"encoding/json"
	"io"
	"net/http"
)

// Reads from the request's query parameters and form fields
func ReadForm(r http.Request, tag string) (crudiator.DataForm, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	form := crudiator.MapBackedDataForm{}
	for k, v := range r.Form {
		form[k] = v[0]
	}
	return form, nil
}

// Reads json data from the request body
func ReadJson(r http.Request) (crudiator.DataForm, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	form := crudiator.MapBackedDataForm{}
	err = json.Unmarshal(data, &form)
	if err != nil {
		return nil, err
	}
	return form, nil
}
