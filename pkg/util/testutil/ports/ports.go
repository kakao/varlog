package ports

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

const (
	ReservationSize = 1000
	MinPort         = 0
	MaxPort         = 65535
)

var (
	errNop  = errors.New("nop")
	tempDir = os.TempDir()
)

type Lease struct {
	base    int
	name    string
	release func() error
	once    sync.Once
}

func (r *Lease) Release() (err error) {
	r.once.Do(func() {
		err = r.release()
	})
	return err
}

func (r *Lease) Base() int {
	return r.base
}

func ReserveWeakly(begin int) (*Lease, error) {
	if MinPort > begin || begin > MaxPort {
		return nil, errors.New("invalid port range")
	}
	if begin%ReservationSize != 0 {
		return nil, fmt.Errorf("begin should be multiple of %d", ReservationSize)
	}

	name := filepath.Join(tempDir, fmt.Sprintf("varlog_ports_pool_%d", begin))
	ua := &net.UnixAddr{Name: name}
	lis, err := net.ListenUnix("unix", ua)
	if err != nil {
		return nil, err
	}
	lease := &Lease{
		base: begin,
		name: name,
		release: func() error {
			return lis.Close()
		},
	}
	return lease, err
}

func ReserveWeaklyWithRetry(begin int) (lease *Lease, err error) {
	err = errNop
	for ; err != nil && begin < MaxPort; begin += ReservationSize {
		lease, err = ReserveWeakly(begin)
	}
	return lease, err
}
