package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
)

func TestSnapshotMarshal(t *testing.T) {
	smr := &mrpb.MetadataRepositoryDescriptor{}
	smr.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}

	for i := 0; i < 128; i++ {
		gls := &mrpb.LogStreamCommitResults{}
		gls.Version = types.Version(i + 1)

		for j := 0; j < 1024; j++ {
			lls := snpb.LogStreamCommitResult{}
			lls.LogStreamID = types.LogStreamID(j)
			lls.CommittedGLSNOffset = types.GLSN(1024*i + j*2)
			lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

			gls.CommitResults = append(gls.CommitResults, lls)
		}

		smr.LogStream.CommitHistory = append(smr.LogStream.CommitHistory, gls)
	}

	_, err := smr.Marshal()
	require.NoError(t, err)
}

func TestGlobalLogStreamMarshal(t *testing.T) {
	gls := &mrpb.LogStreamCommitResults{}
	gls.Version = types.Version(1000)

	for i := 0; i < 128*1024; i++ {
		lls := snpb.LogStreamCommitResult{}
		lls.LogStreamID = types.LogStreamID(i)
		lls.CommittedGLSNOffset = types.GLSN(i)
		lls.CommittedGLSNLength = uint64(lls.CommittedGLSNOffset) + 1

		gls.CommitResults = append(gls.CommitResults, lls)
	}

	_, err := gls.Marshal()
	require.NoError(t, err)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
