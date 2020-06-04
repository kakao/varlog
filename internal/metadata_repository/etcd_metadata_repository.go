package metadata_repository

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	varlogpb "github.com/kakao/varlog/proto/varlog"

	etcdcli "go.etcd.io/etcd/clientv3"
)

type EtcdMetadataRepository struct {
	cli *etcdcli.Client
}

func NewEtcdMetadataRepository() *EtcdMetadataRepository {
	cli, err := etcdcli.New(etcdcli.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil
	}

	kvc := etcdcli.NewKV(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).
		If(etcdcli.Compare(etcdcli.CreateRevision("epoch"), "=", 0)).
		Then(etcdcli.OpPut("epoch", "0")).
		Commit()
	cancel()

	//log.Printf("%+v\n", t)
	if err != nil {
		log.Println(err)
		return nil
	}

	r := &EtcdMetadataRepository{cli: cli}
	return r
}

func (r *EtcdMetadataRepository) Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	body, _ := projection.Marshal()
	pkey := fmt.Sprintf("projection-%020d", epoch+1)

	sepoch := strconv.FormatUint(epoch, 10)
	nepoch := strconv.FormatUint(epoch+1, 10)

	kvc := etcdcli.NewKV(r.cli)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	_, err := kvc.Txn(ctx).
		If(etcdcli.Compare(etcdcli.Value("epoch"), "=", sepoch)).
		Then(etcdcli.OpPut("epoch", nepoch), etcdcli.OpPut(pkey, string(body))).
		Commit()
	cancel()

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (r *EtcdMetadataRepository) Get(epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	pkey := fmt.Sprintf("projection-%020d", epoch)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := r.cli.Get(ctx, pkey)
	cancel()
	if err != nil {
		return nil, err
	}

	var p *varlogpb.ProjectionDescriptor

	if len(resp.Kvs) > 0 {
		ev := resp.Kvs[0]

		p = &varlogpb.ProjectionDescriptor{}
		p.Unmarshal(ev.Value)
	}

	return p, nil
}

func (r *EtcdMetadataRepository) Clear() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	r.cli.Delete(ctx, "epoch")
	r.cli.Delete(ctx, "projection-", etcdcli.WithPrefix())
	cancel()
}
