package benchmark

import (
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz"

func NewMessage(size uint) []byte {
	if size == 0 {
		return []byte{}
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	msg := make([]byte, size)
	for i := range msg {
		msg[i] = charset[rng.Intn(len(charset))]
	}
	return msg
}
