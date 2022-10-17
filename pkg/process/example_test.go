package process_test

import (
	"context"
	"fmt"

	"github.com/kakao/varlog/pkg/process"
)

func ExampleProcess() {
	proc, err := process.New(context.Background(), "sh", "-c", "echo stdout1; echo stdout2; echo 1>&2 stderr1; echo 1>&2 stderr2")
	if err != nil {
		panic(err)
	}

	if err := proc.Start(); err != nil {
		panic(err)
	}

	for line := range proc.Stderr() {
		fmt.Println(line)
	}
	for line := range proc.Stdout() {
		fmt.Println(line)
	}

	if err := proc.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// stderr1
	// stderr2
	// stdout1
	// stdout2
}
