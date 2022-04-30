package first

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

func testStringSlice(nums ...int) []interface{} {
	ret := make([]interface{}, len(nums))
	for i := 0; i < len(nums); i++ {
		ret[i] = strconv.Itoa(nums[i])
	}
	return ret
}

func TestRun(t *testing.T) {
	var tests = []struct {
		in        []interface{}
		outBool   bool
		outValues []int
	}{
		{testStringSlice(1, 3, 5), false, nil},
		{testStringSlice(0, 3, 5), true, []int{0}},
		{testStringSlice(0, 2, 5), true, []int{0, 2}},
	}

	hasEven := func(_ context.Context, x interface{}) (interface{}, error) {
		num, err := strconv.Atoi(x.(string))
		if err != nil {
			t.Error(err)
		}
		even := num%2 == 0
		if even {
			return even, nil
		}
		return even, errors.New("odd number")
	}

	for i := range tests[:3] {
		test := tests[i]
		name := fmt.Sprintf("%v", test.in)
		t.Run(name, func(t *testing.T) {
			ret, err := Run(context.TODO(), test.in, hasEven)
			if test.outBool && err != nil {
				t.Error("should be ok")
				found := false
				for _, v := range test.outValues {
					if v == ret.(int) {
						found = true
					}
				}
				if !found {
					t.Error("invalid output")
				}
			}
			if !test.outBool && err == nil {
				t.Error("should be failed")
			}
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
