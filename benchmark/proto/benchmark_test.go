package proto

import (
	"fmt"
	"testing"
)

func BenchmarkProto(b *testing.B) {
	var buf [1 << 25]byte
	for _, benchType := range []string{"CreateAndMarshal", "OnlyMarshal"} {
		for _, numPayload := range []int{5, 20} {
			for _, sizePayload := range []int{100, 1000} {
				for _, newMessageF := range []struct {
					name string
					f    func(int64, int, int) marshalable
				}{
					{name: "PayloadAHeap", f: newMessagePayloadAHeap},
					{name: "PayloadALessHeap", f: newMessagePayloadALessHeap},
					{name: "PayloadBHeap", f: newMessagePayloadBHeap},
					{name: "PayloadBLessHeap", f: newMessagePayloadBLessHeap},
					{name: "PayloadCHeap", f: newMessagePayloadCHeap},
					{name: "PayloadCLessHeap", f: newMessagePayloadCLessHeap},
					{name: "PayloadASplit", f: newMessagePayloadASplit},
					{name: "PayloadBSplit", f: newMessagePayloadBSplit},
					{name: "PayloadCSplit", f: newMessagePayloadCSplit},
				} {
					var benchTypeF func(*testing.B, []byte, int, int, func(int64, int, int) marshalable)
					if benchType == "CreateAndMarshal" {
						benchTypeF = benchmarkCreateAndMarshal
					} else {
						benchTypeF = benchmarkdMarshal
					}
					name := fmt.Sprintf("%s_%s_num%d_size%d", benchType, newMessageF.name, numPayload, sizePayload)
					b.Run(name, func(b *testing.B) {
						benchTypeF(b, buf[:], numPayload, sizePayload, newMessageF.f)
					})
				}
			}
		}
	}
}

type marshalable interface {
	MarshalTo([]byte) (int, error)
}

func newMessagePayloadAHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadAHeap{
		Payloads: make([]*PayloadA, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i] = &PayloadA{
			Id:   n,
			Data: make([]byte, size),
		}
	}
	return msg
}

func newMessagePayloadALessHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadALessHeap{
		Payloads: make([]PayloadA, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i].Id = n
		msg.Payloads[i].Data = make([]byte, size)
	}
	return msg
}

func newMessagePayloadBHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadBHeap{
		Payloads: make([]*PayloadB, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i] = &PayloadB{
			Id:   n,
			Data: Buffer{Data: make([]byte, size)},
		}
	}
	return msg
}

func newMessagePayloadBLessHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadBLessHeap{
		Payloads: make([]PayloadB, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i].Id = n
		msg.Payloads[i].Data = Buffer{Data: make([]byte, size)}
	}
	return msg
}

func newMessagePayloadCHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadCHeap{
		Payloads: make([]*PayloadC, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i] = &PayloadC{
			Id:   n,
			Data: &Buffer{Data: make([]byte, size)},
		}
	}
	return msg
}

func newMessagePayloadCLessHeap(n int64, num, size int) marshalable {
	msg := &MessagePayloadCLessHeap{
		Payloads: make([]PayloadC, num),
	}
	for i := 0; i < num; i++ {
		msg.Payloads[i].Id = n
		msg.Payloads[i].Data = &Buffer{Data: make([]byte, size)}
	}
	return msg
}

func newMessagePayloadASplit(n int64, num, size int) marshalable {
	msg := &MessagePayloadASplit{
		Ids:   make([]int64, num),
		Datas: make([][]byte, num),
	}
	for i := 0; i < num; i++ {
		msg.Ids[i] = n
		msg.Datas[i] = make([]byte, size)
	}
	return msg
}

func newMessagePayloadBSplit(n int64, num, size int) marshalable {
	msg := &MessagePayloadBSplit{
		Ids:   make([]int64, num),
		Datas: make([]Buffer, num),
	}
	for i := 0; i < num; i++ {
		msg.Ids[i] = n
		msg.Datas[i] = Buffer{Data: make([]byte, size)}
	}
	return msg
}

func newMessagePayloadCSplit(n int64, num, size int) marshalable {
	msg := &MessagePayloadCSplit{
		Ids:   make([]int64, num),
		Datas: make([]*Buffer, num),
	}
	for i := 0; i < num; i++ {
		msg.Ids[i] = n
		msg.Datas[i] = &Buffer{Data: make([]byte, size)}
	}
	return msg
}

func benchmarkCreateAndMarshal(b *testing.B, buf []byte, num, size int, f func(int64, int, int) marshalable) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := f(int64(i), num, size)
		if _, err := msg.MarshalTo(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkdMarshal(b *testing.B, buf []byte, num, size int, f func(int64, int, int) marshalable) {
	msg := f(12345, num, size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.MarshalTo(buf); err != nil {
			b.Fatal(err)
		}
	}
}
