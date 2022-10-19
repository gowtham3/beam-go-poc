package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

// LogFn is a DoFn to log rides.
type LogFn struct{}

func (fn *LogFn) ProcessElement(ctx context.Context, elm []byte) {
	//log.Infof(ctx, "Ride info: %v", string(elm))
}

func main() {
	// In order to start creating the pipeline for execution, a Pipeline object is needed.
	flag.Parse()
	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()
	ctx := context.Background()

	log.Info(ctx, "Starting ")
	//_ = textio.Read(s, "example_input.txt")
	//
	//log.Infof(ctx, "%v", msgs)
	messages := kafkaio.Read(s,
		"localhost:8097",
		"localhost:9092",
		[]string{"quickstart-events"},
	)

	vals := beam.DropKey(s, messages)
	beam.ParDo0(s, &LogFn{}, vals)
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err.Error())
	}
}
