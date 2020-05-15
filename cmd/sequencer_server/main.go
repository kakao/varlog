package main

import (
	"fmt"

	pb "github.daumkakao.com/wokl/wokl/proto/sequencer"
)

type server struct {
	glsn uint64
}

func main() {
	fmt.Println("ok")
	var _ pb.SequencerRequest
}
