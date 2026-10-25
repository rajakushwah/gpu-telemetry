// Command api serves the public REST API for GPU telemetry.
//
//	@title			GPU Telemetry API
//	@version		1.0
//	@description	Query GPU telemetry collected by the elastic telemetry pipeline.
//	@BasePath		/api/v1
package main

import (
	"os"

	_ "github.com/rajakushwah/gpu-telemetry/docs" // registers the generated OpenAPI spec
	"github.com/rajakushwah/gpu-telemetry/internal/api"
	"github.com/rajakushwah/gpu-telemetry/internal/platform"
	"github.com/rajakushwah/gpu-telemetry/internal/store"
)

func main() {
	log := platform.Logger("api")
	ctx, stop := platform.SignalContext()
	defer stop()

	st, err := store.Connect(ctx, platform.Env("STORE", "postgres"), os.Getenv("DATABASE_URL"))
	if err != nil {
		platform.Fatal(log, "connect store", err)
	}
	defer st.Close()

	srv := platform.NewServer(platform.Env("LISTEN_ADDR", ":8080"), api.New(st, log))
	if err := platform.Serve(ctx, srv, log); err != nil {
		log.Error("server stopped", "err", err)
	}
}
