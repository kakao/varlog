//go:build long_e2e
// +build long_e2e

package e2e

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

func TestK8sVarlogAppendLongTime(t *testing.T) {
	const (
		testTimeout  = 15 * time.Minute
		numRepFactor = 3
		numMRs       = 3
		numSNs       = 9
		numLSs       = 3
		numClients   = 10
		clusterID    = types.ClusterID(1)
	)

	opts := getK8sVarlogClusterOpts()
	opts.NrMR = numMRs
	opts.NrSN = numSNs
	opts.NrLS = numLSs
	opts.RepFactor = numRepFactor

	Convey("Append long time", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsAddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		var mcl varlog.ClusterManagerClient
		k8s.WithTimeoutContext(func(ctx context.Context) {
			mcl, err = varlog.NewClusterManagerClient(ctx, vmsAddr)
			So(err, ShouldBeNil)
		})
		Reset(func() {
			So(mcl.Close(), ShouldBeNil)
		})

		for i := 0; i < numLSs; i++ {
			k8s.WithTimeoutContext(func(ctx context.Context) {
				rsp, err := mcl.AddLogStream(ctx, nil)
				So(err, ShouldBeNil)
				if err == nil {
					logStreamID := rsp.GetLogStream().GetLogStreamID()
					log.Printf("AddLogStream: %v", logStreamID)
				}
			})
		}

		mrSeed, err := k8s.MRAddress()
		So(err, ShouldBeNil)
		mrSeeds := []string{mrSeed}

		vlgOpts := []varlog.Option{
			varlog.WithDenyTTL(5 * time.Second),
			varlog.WithMRConnectorCallTimeout(3 * time.Second),
			varlog.WithMetadataRefreshTimeout(3 * time.Second),
			varlog.WithOpenTimeout(10 * time.Second),
		}

		tctx, tcancel := context.WithTimeout(context.TODO(), testTimeout)
		defer tcancel()
		grp, ctx := errgroup.WithContext(tctx)
		for i := 0; i < numClients; i++ {
			clientIdx := i + 1
			grp.Go(func() (err error) {
				startTime := time.Now()
				log.Printf("client-%d starts", clientIdx)
				var vlg varlog.Varlog
				k8s.WithTimeoutContext(func(ctx context.Context) {
					vlg, err = varlog.Open(ctx, clusterID, mrSeeds, vlgOpts...)
				})
				if err != nil {
					return err
				}

				n := int64(0)
				defer func() {
					err = multierr.Append(err, vlg.Close())
					elapsedTime := time.Since(startTime)
					log.Printf("client-%d appended %d messages for %s.", clientIdx, n, elapsedTime.String())
				}()

				for ctx.Err() == nil {
					data := fmt.Sprintf("client-%d-log-%d", clientIdx, n)
					k8s.WithTimeoutContext(func(ctx context.Context) {
						_, err = vlg.Append(ctx, []byte(data))
						if err == nil {
							n++
						}
					})
					if err != nil {
						break
					}
				}

				return err
			})
		}
		if err = grp.Wait(); err != nil {
			t.Logf("err=%+v", err)
		}
		So(err, ShouldBeNil)
	}))
}
