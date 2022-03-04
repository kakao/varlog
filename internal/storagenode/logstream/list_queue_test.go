package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestListQueueBack(t *testing.T) {
	defer goleak.VerifyNone(t)

	lq := newListQueue()
	defer lq.release()

	assert.Zero(t, lq.Len())
	assert.Nil(t, lq.Back())
	assert.Nil(t, lq.RemoveBack())

	lq.PushFront(1)
	assert.Equal(t, 1, lq.Len())
	lq.PushFront(2)
	assert.Equal(t, 2, lq.Len())
	assert.EqualValues(t, 1, lq.Back().value)
	assert.EqualValues(t, 1, lq.RemoveBack())
	assert.Equal(t, 1, lq.Len())
	assert.EqualValues(t, 2, lq.Back().value)
}

func TestListQueueConcatFront(t *testing.T) {
	defer goleak.VerifyNone(t)

	lq1 := newListQueue()
	defer lq1.release()
	lq1.PushFront(1)
	lq1.PushFront(2)
	assert.Equal(t, 2, lq1.Len())

	lq2 := newListQueue()
	defer lq2.release()
	assert.Zero(t, lq2.Len())

	lq1.ConcatFront(lq2)
	assert.Equal(t, 2, lq1.Len())

	lq3 := newListQueue()
	defer lq3.release()
	lq3.PushFront(3)
	assert.Equal(t, 1, lq3.Len())

	lq1.ConcatFront(lq3)
	assert.Equal(t, 3, lq1.Len())
	assert.Zero(t, lq3.Len())
}

func TestListQueueConcatFrontAndRemoveBack(t *testing.T) {
	defer goleak.VerifyNone(t)

	lq := newListQueue()
	defer lq.release()

	for i := 0; i < 10; i++ {
		tmp := newListQueue()
		for j := 0; j < 1000; j++ {
			tmp.PushFront(j)
		}
		lq.ConcatFront(tmp)
		tmp.release()
	}

	for lq.Back() != nil {
		assert.NotNil(t, lq.RemoveBack())
	}
}

func TestListQueue(t *testing.T) {
	defer goleak.VerifyNone(t)

	// <Front> 3 -> 2 -> 1 <Back>
	lst1 := newListQueue()
	defer lst1.release()
	lst1.PushFront(1)
	lst1.PushFront(2)
	lst1.PushFront(3)

	// 1
	lBack := lst1.Back()
	assert.EqualValues(t, 1, lBack.value)

	// 2
	lBack = lBack.Prev()
	assert.EqualValues(t, 2, lBack.value)

	// 3
	lBack = lBack.Prev()
	assert.EqualValues(t, 3, lBack.value)

	// nil
	lBack = lBack.Prev()
	assert.Nil(t, lBack)

	// nil
	lBack = lBack.Prev()
	assert.Nil(t, lBack)

	// <Front> 3 -> 2 <Back>
	assert.EqualValues(t, 1, lst1.RemoveBack())

	// <LastIn> 6 -> 5 -> 4 <FirstIn>
	lst2 := newListQueue()
	lst2.PushFront(4)
	lst2.PushFront(5)
	lst2.PushFront(6)

	// <LastIn> 6 -> 5 -> 4 -> 3 -> 2 <FirstIn>
	lst1.ConcatFront(lst2)
	lst2.release()
	back := lst1.Back()
	expected := 2
	for back != nil {
		assert.EqualValues(t, expected, back.value)
		expected++
		back = back.Prev()
	}

	// <LastIn> 8 -> 7 <FirstIn>
	lst2 = newListQueue()
	lst2.PushFront(7)
	lst2.PushFront(8)

	// <LastIn> 8 -> 7 -> 6 -> 5 -> 4 -> 3 -> 2 <FirstIn>
	lst1.ConcatFront(lst2)
	lst2.release()
	assert.Equal(t, 7, lst1.Len())
	back = lst1.Back()
	expected = 2
	for back != nil {
		assert.EqualValues(t, expected, back.value)
		expected++
		back = back.Prev()
	}

	lst3 := newListQueue()
	defer lst3.release()
	assert.Zero(t, lst3.Len())

	lst3.ConcatFront(lst1)
	assert.Equal(t, 7, lst3.Len())

	back = lst3.Back()
	expected = 2
	for back != nil {
		assert.EqualValues(t, expected, back.value)
		expected++
		back = back.Prev()
	}

	expected = 2
	assert.Equal(t, 7, lst3.Len())
	cnt := lst3.Len()
	for i := 0; i < cnt; i++ {
		assert.EqualValues(t, expected, lst3.RemoveBack())
		expected++
	}
	assert.Zero(t, lst3.Len())
}
