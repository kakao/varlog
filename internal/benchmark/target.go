package benchmark

import (
	"errors"
	"fmt"

	"github.com/kakao/varlog/pkg/types"
)

type Target struct {
	TopicID          types.TopicID     `json:"topicId"`
	LogStreamID      types.LogStreamID `json:"logStreamId"`
	MessageSize      uint
	BatchSize        uint
	AppendersCount   uint
	SubscribersCount uint
	PipelineSize     int
}

func (tgt Target) Valid() error {
	if tgt.TopicID.Invalid() {
		return fmt.Errorf("invalid topic %v", tgt.TopicID)
	}
	if tgt.BatchSize < 1 {
		return fmt.Errorf("batch size %d", tgt.BatchSize)
	}
	if tgt.AppendersCount == 0 && tgt.SubscribersCount == 0 {
		return errors.New("no appenders and subscribers")
	}
	return nil
}

func (tgt Target) String() string {
	ret := tgt.TopicID.String()
	if tgt.LogStreamID.Invalid() {
		ret += ":*"
	} else {
		ret += ":" + tgt.LogStreamID.String()
	}
	return ret
}
