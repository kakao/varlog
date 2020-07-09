package main

import (
	"log"
	"testing"
	"time"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	mrpb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
)

func TestSnapshotMarshal(t *testing.T) {
	smr := &mrpb.MetadataRepositoryDescriptor{}
	smr.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}

	for i := 0; i < 128; i++ {
		gls := &snpb.GlobalLogStreamDescriptor{}
		gls.NextGLSN = types.GLSN((i + 1) * 16)
		gls.PrevNextGLSN = types.GLSN(i * 16)

		for j := 0; j < 1024; j++ {
			lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
			lls.LogStreamID = types.LogStreamID(j)
			lls.CommittedGLSNBegin = gls.PrevNextGLSN + types.GLSN(j*2)
			lls.CommittedGLSNEnd = lls.CommittedGLSNBegin + 1

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
	gls.NextGLSN = types.GLSN(1000)
	gls.PrevNextGLSN = types.GLSN(16)

	for i := 0; i < 128*1024; i++ {
		lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{}
		lls.LogStreamID = types.LogStreamID(i)
		lls.CommittedGLSNBegin = gls.PrevNextGLSN + types.GLSN(i*2)
		lls.CommittedGLSNEnd = lls.CommittedGLSNBegin + 1

		gls.CommitResult = append(gls.CommitResult, lls)
	}
	st := time.Now()

	gls.Marshal()

	log.Println(time.Now().Sub(st))
}
