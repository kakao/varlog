package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	varlogpb "github.com/kakao/varlog/proto/varlog"

	etcdcli "go.etcd.io/etcd/clientv3"
)

type EtcdProxyMetadataRepository struct {
	cli      *etcdcli.Client
	epoch    uint64
	metadata varlogpb.MetadataDescriptor
	mu       sync.Mutex
}

func getProjectionKey(epoch uint64) string {
	return fmt.Sprintf("projection-%020d", epoch)
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

	r := &EtcdProxyMetadataRepository{
		cli: cli,
	}

	err = r.fetchMetadata()
	if err != nil {
		return nil
	}

	return r
}

func (r *EtcdProxyMetadataRepository) fetchMetadata() error {
	if err := r.fetchEpoch(); err != nil {
		return err
	}

	if err := r.fetchSequencer(); err != nil {
		return err
	}

	if err := r.fetchProjections(); err != nil {
		return err
	}

	if err := r.fetchStorageNodes(); err != nil {
		return err
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchEpoch() error {
	kvc := etcdcli.NewKV(r.cli)

	if r.epoch == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := kvc.Txn(ctx).
			If(etcdcli.Compare(etcdcli.CreateRevision("epoch"), "=", 0)).
			Then(etcdcli.OpPut("epoch", "0")).
			Commit()
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := kvc.Get(ctx, "epoch")
	if err != nil {
		return err
	}

	if len(resp.Kvs) > 0 {
		v := resp.Kvs[0].Value

		e, _ := strconv.ParseUint(string(v), 10, 64)
		r.epoch = e
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchSequencer() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := r.cli.Get(ctx, "sequencer")
	if err != nil {
		return err
	}

	if len(resp.Kvs) > 0 {
		v := resp.Kvs[0].Value

		s := &varlogpb.SequencerDescriptor{}
		s.Unmarshal(v)

		r.metadata.Sequencer = *s
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchProjections() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := r.cli.Get(ctx, "projection-", etcdcli.WithPrefix(), etcdcli.WithSort(etcdcli.SortByKey, etcdcli.SortAscend))
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		p := &varlogpb.ProjectionDescriptor{}
		p.Unmarshal(ev.Value)

		r.metadata.Projections = append(r.metadata.Projections, *p)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchProjectionsWithRange(start, end uint64) error {
	skey := getProjectionKey(start)
	ekey := getProjectionKey(end)

	var resp *etcdcli.GetResponse
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if start < end {
		resp, err = r.cli.Get(ctx, skey, etcdcli.WithRange(ekey), etcdcli.WithSort(etcdcli.SortByKey, etcdcli.SortAscend))
	} else {
		resp, err = r.cli.Get(ctx, skey)
	}
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		p := &varlogpb.ProjectionDescriptor{}
		p.Unmarshal(ev.Value)

		r.metadata.Projections = append(r.metadata.Projections, *p)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchStorageNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := r.cli.Get(ctx, "storage-", etcdcli.WithPrefix())
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		s := &varlogpb.StorageNodeDescriptor{}
		s.Unmarshal(ev.Value)

		r.metadata.StorageNodes = append(r.metadata.StorageNodes, *s)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	body, _ := projection.Marshal()
	pkey := getProjectionKey(epoch + 1)

	sepoch := strconv.FormatUint(epoch, 10)
	nepoch := strconv.FormatUint(epoch+1, 10)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.metadata.GetLastEpoch() > epoch {
		return nil
	}

	kvc := etcdcli.NewKV(r.cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		r.metadata.Projections = append(r.metadata.Projections, *projection)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) GetProjection(epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.epoch < epoch {
		if err := r.fetchEpoch(); err != nil {
			return nil, err
		}

		if r.epoch < epoch {
			return nil, errors.New("invalid epoch")
		}

		lastEpoch := r.metadata.GetLastEpoch()
		if err := r.fetchProjectionsWithRange(lastEpoch+1, r.epoch+1); err != nil {
			return nil, err
		}
	}

	return r.metadata.GetProjection(epoch), nil
}

func (r *EtcdProxyMetadataRepository) GetMetadata() (*varlogpb.MetadataDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return &r.metadata, nil
}

func (r *EtcdProxyMetadataRepository) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.cli.Delete(ctx, "projection-", etcdcli.WithPrefix())
	r.cli.Put(ctx, "epoch", "0")

	r.epoch = 0
	r.metadata = varlogpb.MetadataDescriptor{}
}

func (r *EtcdProxyMetadataRepository) RegisterSequencer(addr string) error {
	return nil
}

func (r *EtcdProxyMetadataRepository) RegisterStorage(addr, path string, total uint64) error {
	return nil
}

func (r *EtcdProxyMetadataRepository) UnregisterStorage(addr, path string) error {
	return nil
}

func (r *EtcdProxyMetadataRepository) UpdateStorage(addr, path string, used uint64) error {
	return nil
}
