package topic

import (
	"context"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/result"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

const resourceType = "topic"

// Add returns a function to add a new topic.
//
// The result of the function executed successfully serializes to follow:
//      {
//              "data": [
//                      {
//                              "topicId": 1
//                      }
//              ]
//      }
//
// If it fails, serialization results in:
//      {
//              "error": "reason"
//      }
func Add() varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		if td, err := adm.AddTopic(ctx); err != nil {
			res.AddErrors(err)
		} else {
			res.AddDataItems(td)
		}
		return res
	}
}

// Remove returns a function to remove the topic identified with id.
//
// The result of the function executed successfully serializes to follow:
//      {
//              "data": []
//      }
//
// If it fails, serialization results in:
//      {
//              "error": "reason"
//      }
func Remove(id types.TopicID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		_, err := adm.UnregisterTopic(ctx, id)
		if err != nil {
			res.AddErrors(err)
			return res
		}
		return res
	}
}

// Describe returns a function to list of topics or to get the topic identified with id.
//
// The result of the function executed successfully serializes to follow:
//      {
//              "data": [
//                      {
//                              "topicId": 1,
//                              "logStreams": [1, 2]
//                      },
//                      {
//                              "topicId": 2,
//                              "logStreams": [3, 4]
//                      }
//              ]
//      }
//
// If it fails, serialization results in:
//      {
//              "error": "reason"
//      }
func Describe(id ...types.TopicID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		tds, err := adm.Topics(ctx)
		if err != nil {
			res.AddErrors(err)
			return res
		}
		for _, td := range tds {
			if len(id) > 0 && id[0] != td.TopicID {
				continue
			}
			res.AddDataItems(td)
			if len(id) > 0 {
				break
			}
		}
		if len(id) > 0 && res.NumberOfDataItem() == 0 {
			res.AddErrors(errors.Errorf("no such topic %d", id[0]))
		}
		return res
	}
}
