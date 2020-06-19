package main

import (
	"log"
	"testing"
	"time"

	types "github.com/kakao/varlog/pkg/varlog/types"
	mrpb "github.com/kakao/varlog/proto/metadata_repository"
	snpb "github.com/kakao/varlog/proto/storage_node"
)

func TestSnapshotMarshal(t *testing.T) {
	smr := &mrpb.MetadataRepositoryDescriptor{}

	for i := 0; i < 128; i++ {
		gls := &snpb.GlobalLogStreamDescriptor{}
		gls.HighestGlsn = types.GLSN((i + 1) * 16)
		gls.PrevHighestGlsn = types.GLSN(i * 16)

		for j := 0; j < 1024; j++ {
			lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
			lls.LogStreamId = types.LogStreamID(j)
			lls.CommittedGlsnBegin = gls.PrevHighestGlsn + types.GLSN(j*2)
			lls.CommittedGlsnEnd = lls.CommittedGlsnBegin + 1

			gls.CommitResult = append(gls.CommitResult, lls)
		}

		smr.GlobalLogStreams = append(smr.GlobalLogStreams, gls)
	}

	st := time.Now()

	smr.Marshal()

	log.Println(time.Now().Sub(st))
}

func TestGlobalLogStreamMarshal(t *testing.T) {
	gls := &snpb.GlobalLogStreamDescriptor{}
	gls.HighestGlsn = types.GLSN(1000)
	gls.PrevHighestGlsn = types.GLSN(16)

	for i := 0; i < 5000; i++ {
		lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
		lls.LogStreamId = types.LogStreamID(i)
		lls.CommittedGlsnBegin = gls.PrevHighestGlsn + types.GLSN(i*2)
		lls.CommittedGlsnEnd = lls.CommittedGlsnBegin + 1

		gls.CommitResult = append(gls.CommitResult, lls)
	}
	st := time.Now()

	gls.Marshal()

	log.Println(time.Now().Sub(st))
}
