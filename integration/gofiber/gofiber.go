package gofiber

import (
	"SharkFourSix/crudiator"
	"encoding/json"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Reads from the request's query parameters and form fields
func ReadForm(r *fiber.Ctx, tag string) (crudiator.DataForm, error) {
	form := crudiator.MapBackedDataForm{}
	fn := func(m map[string]string) {
		for k, v := range m {
			form[k] = v
		}
	}
	fn(r.Queries())
	r.Context().PostArgs().VisitAll(func(key, value []byte) {
		form[strings.Clone(string(key))] = string(value)
	})
	return form, nil
}

// Reads json data from the request body
func ReadJson(r *fiber.Ctx) (crudiator.DataForm, error) {
	form := crudiator.MapBackedDataForm{}
	err := json.Unmarshal(r.BodyRaw(), &form)
	if err != nil {
		return nil, err
	}
	return form, nil
}
