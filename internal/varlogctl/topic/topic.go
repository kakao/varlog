package topic

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

// Add returns a function to add a new topic.
func Add() varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.AddTopic(ctx)
	}
}

// Remove returns a function to remove the topic identified with id.
func Remove(id types.TopicID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if err := adm.UnregisterTopic(ctx, id); err != nil {
			return nil, err
		}
		return empty.Empty{}, nil
	}
}

// Describe returns a function to list of topics or to get the topic identified with id.
func Describe(id ...types.TopicID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if len(id) > 0 {
			return adm.GetTopic(ctx, id[0])
		}
		return adm.ListTopics(ctx)
	}
}
