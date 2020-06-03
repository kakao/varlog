package main

/*
import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	libvarlog "github.daumkakao.com/varlog/varlog/pkg/libvarlog"
)

const (
	numClients   = 50
	numNextCalls = 10000
)

type report struct {
	numSend int
	numRecv int
}

func do_next(conn *libvarlog.SequencerConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	rpt := report{}
	for i := 0; i < numNextCalls; i++ {
		var glsn uint64
		err := conn.Next(ctx, &glsn)
		if err != nil {
			log.Fatalf("failed to do next: %v", err)
		}
		rpt.numRecv += 1
		//log.Printf("GLSN: %v\n", glsn)
	}
	fmt.Printf("report: %v\n", rpt)
}

func start_client(client *sequencer.SequencerClient, wg *sync.WaitGroup) {
	defer wg.Done()
	do_next(client.Connect())
}

func main() {
	client, err := sequencer.NewSequencerClient("localhost:9091")
	if err != nil {
		log.Fatalf("could not create client: %v", err)
	}
	defer client.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go start_client(client, &wg)
	}
	wg.Wait()
	end := time.Now()
	elapsed := end.Sub(start)
	log.Printf("elapsed = %v ms", elapsed.Milliseconds())
	qps := float64(numClients*numNextCalls) / elapsed.Seconds()
	log.Printf("QPS = %v", qps)
}
*/

func main() {
}
