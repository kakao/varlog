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
		panic(err)
	}

	r := &EtcdProxyMetadataRepository{
		cli: cli,
	}

	err = r.fetchMetadata()
	if err != nil {
		panic(err)
	}

	return r
}

func (r *EtcdProxyMetadataRepository) fetchMetadata() error {
	if err := r.fetchEpoch(); err != nil {
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

		e, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}

		r.epoch = e
	} else {
		return errors.New("No epoch info")
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
		err := p.Unmarshal(ev.Value)
		if err != nil {
			return err
		}

		r.metadata.Projections = append(r.metadata.Projections, *p)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchProjectionsWithRange(start, end uint64) error {
	/* if start < end, get [start, end)
	*  else if start == end, get start */
	if start < end {
		return nil
	}

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
		err := p.Unmarshal(ev.Value)
		if err != nil {
			return err
		}

		r.metadata.Projections = append(r.metadata.Projections, *p)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) fetchEpochProjections() error {
	epoch := r.metadata.GetLastEpoch()

	if err := r.fetchEpoch(); err != nil {
		return err
	}

	if err := r.fetchProjectionsWithRange(epoch+1, r.epoch+1); err != nil {
		return err
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
		err := s.Unmarshal(ev.Value)
		if err != nil {
			return err
		}

		r.metadata.StorageNodes = append(r.metadata.StorageNodes, *s)
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	body, err := projection.Marshal()
	if err != nil {
		return err
	}

	pkey := getProjectionKey(epoch + 1)

	sepoch := strconv.FormatUint(epoch, 10)
	nepoch := strconv.FormatUint(epoch+1, 10)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.metadata.GetLastEpoch() > epoch {
		return nil
	}

	lastEpoch := r.metadata.GetLastEpoch()

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
		if lastEpoch == epoch {
			r.metadata.Projections = append(r.metadata.Projections, *projection)
		} else {
			// epoch > lastEpoch, local epoch 가 최신 epoch 가 아니었으므로 update 한다.
			if err := r.fetchEpochProjections(); err != nil {
				return err
			}
		}
	} else if lastEpoch == epoch {
		// local epoch 가 최신 epoch 가 아니므로 update 한다.
		if err := r.fetchEpochProjections(); err != nil {
			return err
		}
	}

	return nil
}

func (r *EtcdProxyMetadataRepository) GetProjection(epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.epoch < epoch {
		if err := r.fetchEpochProjections(); err != nil {
			return nil, err
		}

		if r.epoch < epoch {
			return nil, errors.New("invalid epoch")
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
	return errors.New("not yet implemented")
}

func (r *EtcdProxyMetadataRepository) RegisterStorage(addr, path string, total uint64) error {
	return errors.New("not yet implemented")
}

func (r *EtcdProxyMetadataRepository) UnregisterStorage(addr, path string) error {
	return errors.New("not yet implemented")
}

func (r *EtcdProxyMetadataRepository) UpdateStorage(addr, path string, used uint64) error {
	return errors.New("not yet implemented")
}
