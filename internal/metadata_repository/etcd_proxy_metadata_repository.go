package metadata_repository

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	etcdcli "go.etcd.io/etcd/clientv3"
)

type EtcdProxyMetadataRepository struct {
	cli         *etcdcli.Client
	epoch       uint64
	projections map[uint64]*varlogpb.ProjectionDescriptor
	mu          sync.Mutex
}

func NewEtcdProxyMetadataRepository() *EtcdProxyMetadataRepository {
	cli, err := etcdcli.New(etcdcli.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Println(err)
		return nil
	}

	projections := make(map[uint64]*varlogpb.ProjectionDescriptor)
	epoch := uint64(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, "epoch")
	if err != nil {
		return nil
	}

	if len(resp.Kvs) > 0 {
		v := resp.Kvs[0].Value

		epoch, _ = strconv.ParseUint(string(v), 10, 64)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err = cli.Put(ctx, "epoch", "0")
		if err != nil {
			return nil
		}

		epoch = 0
	}

	if epoch > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := cli.Get(ctx, "projection-", etcdcli.WithPrefix())
		if err != nil {
			return nil
		}

		for _, ev := range resp.Kvs {
			e, _ := strconv.ParseUint(strings.Split(string(ev.Key), "-")[1], 10, 64)

			p := &varlogpb.ProjectionDescriptor{}
			p.Unmarshal(ev.Value)

			projections[e] = p
		}
	}

	r := &EtcdProxyMetadataRepository{
		cli:         cli,
		epoch:       epoch,
		projections: projections,
	}

	return r
}

func (r *EtcdProxyMetadataRepository) Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	body, _ := projection.Marshal()
	pkey := fmt.Sprintf("projection-%020d", epoch+1)

	sepoch := strconv.FormatUint(epoch, 10)
	nepoch := strconv.FormatUint(epoch+1, 10)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.epoch > epoch {
		return nil
	}

	kvc := etcdcli.NewKV(r.cli)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	t, err := kvc.Txn(ctx).
		If(etcdcli.Compare(etcdcli.Value("epoch"), "=", sepoch)).
		Then(etcdcli.OpPut("epoch", nepoch), etcdcli.OpPut(pkey, string(body))).
		Commit()
	if err != nil {
		log.Println(err)
		return err
	}

	if t.Succeeded {
		r.epoch = epoch + 1
		r.projections[r.epoch] = projection
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) Get(epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.epoch < epoch {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := r.cli.Get(ctx, "epoch")
		if err != nil {
			return nil, err
		}

		if len(resp.Kvs) > 0 {
			v := resp.Kvs[0].Value

			e, _ := strconv.ParseUint(string(v), 10, 64)
			r.epoch = e
		}

		if r.epoch < epoch {
			return nil, nil
		}
	}

	if p, ok := r.projections[epoch]; ok {
		return p, nil
	}

	pkey := fmt.Sprintf("projection-%020d", epoch)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := r.cli.Get(ctx, pkey)
	if err != nil {
		return nil, err
	}

	var p *varlogpb.ProjectionDescriptor

	if len(resp.Kvs) > 0 {
		ev := resp.Kvs[0]

		p = &varlogpb.ProjectionDescriptor{}
		p.Unmarshal(ev.Value)

		r.projections[epoch] = p
	} else {
		log.Printf("can't find porjection %d. cur epoch:%d\n", epoch, r.epoch)
	}

	return p, nil
}

func (r *EtcdProxyMetadataRepository) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.cli.Delete(ctx, "projection-", etcdcli.WithPrefix())
	r.cli.Put(ctx, "epoch", "0")

	r.epoch = 0
	r.projections = make(map[uint64]*varlogpb.ProjectionDescriptor)

	log.Printf("etcd clear. epoch:0\n")
}
