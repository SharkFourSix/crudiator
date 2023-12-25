package crudiator_test

import (
	"SharkFourSix/crudiator"
	"testing"
)

func TestForm(t *testing.T) {
	myStruct := struct {
		Name     string `json:"-"`
		NickName string `json:"nick"`
		Age      int    `json:"age"`
		Dob      string
	}{
		Name:     "John Doe",
		NickName: "Johnny",
		Age:      25,
		Dob:      "",
	}

	form := crudiator.FromJsonStruct(&myStruct)

	if form.Get("age") != 25 {
		t.Fatal("json field fail")
	}
	if form.Get("nick") != "Johnny" {
		t.Fatal("json field fail")
	}
	if form.Has("name") || form.Has("Dob") {
		t.Fatal("skip field failed")
	}
}
