package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"../sequencer"
	"google.golang.org/grpc"
)

const (
	numClients   = 10
	numNextCalls = 100000
)

type report struct {
	numSend int
	numRecv int
}

func do_next(client sequencer.SequencerServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	rpt := report{}
	for i := 0; i < numNextCalls; i++ {
		r, err := client.Next(ctx, &sequencer.SequencerRequest{})
		if err != nil {
			log.Fatalf("failed to do next: %v", err)
		}
		rpt.numRecv += 1
		log.Printf("GLSN: %v\n", r.Glsn)
	}

	/*
		stream, err := client.Next(ctx)
		if err != nil {
			log.Fatalf("%v.Next(_) = _, %v", client, err)
		}


		waitc := make(chan struct{})
		go func() {
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					close(waitc)
					return
				}
				if err != nil {
					log.Fatalf("failed to receive: %v", err)
				}
				// ok message:
				rpt.numRecv += 1
				// fmt.Printf("GLSN: %v\n", rsp.Glsn)
			}
		}()

		for i := 0; i < numNextCalls; i++ {
			request := &sequencer.SequencerRequest{}
			if err := stream.Send(request); err != nil {
				log.Fatalf("failed to send: %v", err)
			}
			rpt.numSend += 1
		}
		stream.CloseSend()
		<-waitc
	*/
	fmt.Printf("report: %v\n", rpt)
}

func start_client(wg *sync.WaitGroup) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial("localhost:9091", opts...)
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	defer wg.Done()
	client := sequencer.NewSequencerServiceClient(conn)
	do_next(client)
}

func main() {
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go start_client(&wg)
	}
	wg.Wait()
	end := time.Now()
	elapsed := end.Sub(start)
	log.Printf("elapsed = %v ms", elapsed.Milliseconds())
	qps := float64(numClients*numNextCalls) / elapsed.Seconds()
	log.Printf("QPS = %v", qps)
}
