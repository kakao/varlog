package test

import (
	"log"
	"testing"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

func TestSnapshotMarshal(t *testing.T) {
	smr := &mrpb.MetadataRepositoryDescriptor{}
	smr.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}

	for i := 0; i < 128; i++ {
		gls := &mrpb.LogStreamCommitResults{}
		gls.HighWatermark = types.GLSN((i + 1) * 16)
		gls.PrevHighWatermark = types.GLSN(i * 16)

		for j := 0; j < 1024; j++ {
			lls := snpb.LogStreamCommitResult{}
			lls.LogStreamID = types.LogStreamID(j)
			lls.CommittedGLSNOffset = gls.PrevHighWatermark + types.GLSN(j*2)
			lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

			gls.CommitResults = append(gls.CommitResults, lls)
		}

		smr.LogStream.CommitHistory = append(smr.LogStream.CommitHistory, gls)
	}

	st := time.Now()

	smr.Marshal()

	log.Println(time.Now().Sub(st))
}

func TestGlobalLogStreamMarshal(t *testing.T) {
	gls := &mrpb.LogStreamCommitResults{}
	gls.HighWatermark = types.GLSN(1000)
	gls.PrevHighWatermark = types.GLSN(16)

	for i := 0; i < 128*1024; i++ {
		lls := snpb.LogStreamCommitResult{}
		lls.LogStreamID = types.LogStreamID(i)
		lls.CommittedGLSNOffset = gls.PrevHighWatermark + types.GLSN(i*2)
		lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

		gls.CommitResults = append(gls.CommitResults, lls)
	}
	st := time.Now()

	gls.Marshal()

	log.Println(time.Now().Sub(st))
}
