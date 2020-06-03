package varlog

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kakao/varlog/pkg/varlog/mock"
	"github.com/kakao/varlog/proto/sequencer"
)

func TestNext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := mock.NewMockSequencerServiceClient(ctrl)
	mockClient.EXPECT().Next(
		gomock.Any(),
		gomock.Any(),
	).Return(&sequencer.SequencerResponse{}, nil)

	_, err := mockClient.Next(context.Background(), &sequencer.SequencerRequest{})
	if err != nil {
		t.Error()
	}
}
