package crudiator

import (
	"reflect"

	"github.com/pkg/errors"
)

type DataForm interface {
	Has(name string) bool
	Get(name string) any
	Set(name string, value any)
	Remove(name string)
	Iterate(func(key string, value any))
}

func mustBeAStructPointer(ptr any) {
	t := reflect.TypeOf(ptr)
	if t.Kind() != reflect.Ptr {
		panic(errors.Errorf("value must be a pointer"))
	}
	if t.Elem().Kind() != reflect.Struct {
		panic(errors.Errorf("value must be a pointer to a struct"))
	}
}

// convert struct to DataForm
func FromJsonStruct(structptr any, additional ...DataForm) DataForm {
	return FromStruct(structptr, "json", additional...)
}

func FromXmlStruct(structptr any, additional ...DataForm) DataForm {
	return FromStruct(structptr, "xml", additional...)
}

// Create form from a struct. Pass an additional form to append/overwrite values
func FromStruct(structptr any, tag string, additional ...DataForm) DataForm {
	mustBeAStructPointer(structptr)
	var form MapBackedDataForm = make(map[string]any)

	// Get the reflect.Value of the struct
	val := reflect.ValueOf(structptr).Elem()

	// Get the reflect.Type of the struct
	typ := val.Type()

	// Iterate through the struct fields
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Check if the field has the specified tag
		tagValue, found := fieldType.Tag.Lookup(tag)
		if found {
			if tagValue != "-" {
				form.Set(tagValue, field.Interface())
			}
		}
	}

	if len(additional) > 0 {
		df := additional[0]
		df.Iterate(func(key string, value any) {
			form[key] = value
		})
	}

	return form
}

type MapBackedDataForm map[string]any

func (f MapBackedDataForm) Has(name string) bool {
	_, ok := f[name]
	return ok
}
func (f MapBackedDataForm) Get(name string) any {
	return f[name]
}
func (f MapBackedDataForm) Set(name string, value any) {
	f[name] = value
}

func (f MapBackedDataForm) Remove(name string) {
	delete(f, name)
}

func (f MapBackedDataForm) Iterate(fn func(key string, value any)) {
	for k, v := range f {
		fn(k, v)
	}
}
