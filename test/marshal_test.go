package test

import (
	"log"
	"testing"
	"time"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
)

func TestSnapshotMarshal(t *testing.T) {
	smr := &mrpb.MetadataRepositoryDescriptor{}
	smr.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}

	for i := 0; i < 128; i++ {
		gls := &snpb.GlobalLogStreamDescriptor{}
		gls.HighWatermark = types.GLSN((i + 1) * 16)
		gls.PrevHighWatermark = types.GLSN(i * 16)

		for j := 0; j < 1024; j++ {
			lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
			lls.LogStreamID = types.LogStreamID(j)
			lls.CommittedGLSNOffset = gls.PrevHighWatermark + types.GLSN(j*2)
			lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

			gls.CommitResult = append(gls.CommitResult, lls)
		}

		smr.LogStream.GlobalLogStreams = append(smr.LogStream.GlobalLogStreams, gls)
	}

	st := time.Now()

	smr.Marshal()

	log.Println(time.Now().Sub(st))
}

func TestGlobalLogStreamMarshal(t *testing.T) {
	gls := &snpb.GlobalLogStreamDescriptor{}
	gls.HighWatermark = types.GLSN(1000)
	gls.PrevHighWatermark = types.GLSN(16)

	for i := 0; i < 128*1024; i++ {
		lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
		lls.LogStreamID = types.LogStreamID(i)
		lls.CommittedGLSNOffset = gls.PrevHighWatermark + types.GLSN(i*2)
		lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

		gls.CommitResult = append(gls.CommitResult, lls)
	}
	st := time.Now()

	gls.Marshal()

	log.Println(time.Now().Sub(st))
}
