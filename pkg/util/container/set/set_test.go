package set

import (
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStringSet(t *testing.T) {
	ss := New(0)
	for i := 0; i < 10; i++ {
		ss.Add(strconv.Itoa(i))
		if ss.Size() != i+1 {
			t.Error("incorrect add")
		}
		if !ss.Contains(strconv.Itoa(i)) {
			t.Error("incorrect find")
		}
		if ss.Contains(i) {
			t.Error("incorrect find")
		}
	}
	for i := 0; i < 10; i++ {
		ss.Add(strconv.Itoa(i))
		if ss.Size() != 10 {
			t.Error("incorrect add")
		}
	}
	var nums []int
	ss.Foreach(func(k interface{}) bool {
		num, err := strconv.Atoi(k.(string))
		if err != nil {
			t.Error(err)
		}
		if num%2 == 0 {
			nums = append(nums, num)
		}
		return true
	})
	for _, num := range nums {
		if num%2 != 0 {
			t.Error("incorrect foreach")
		}
	}

	nums = nil
	ss.Foreach(func(k interface{}) bool {
		num, err := strconv.Atoi(k.(string))
		if err != nil {
			t.Error(err)
		}
		if num > 5 {
			nums = append(nums, num)
			return false
		}
		return true
	})
	if len(nums) != 1 {
		t.Error("incorrect foreach")
	}
	if nums[0] <= 5 {
		t.Error("incorrect foreach")
	}

	ss.Foreach(func(k interface{}) bool {
		num, err := strconv.Atoi(k.(string))
		if err != nil {
			t.Error(err)
		}
		if num%2 == 0 {
			ss.Remove(k)
		}
		return true
	})

	if ss.Size() != 5 {
		t.Error("incorrect remove")
	}
	ss.Foreach(func(k interface{}) bool {
		num, err := strconv.Atoi(k.(string))
		if err != nil {
			t.Error(err)
		}
		if num%2 == 0 {
			t.Error("incorrect remove")
		}
		return true
	})
}
