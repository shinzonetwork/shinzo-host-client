package main

import (
	"flag"
	"fmt"

	"github.com/shinzonetwork/host/pkg/host"
)

func main() {
	defraStorePath := flag.String("defra-store-path", "", "Path to Defra store directory. If empty, assumes Defra is already running. Example: -defra-store-path=./.defra")
	defraUrl := flag.String("defra-url", "http://localhost:9181", "The URL your defra instance is running on. If you are not currently running a defra instance, please omit this flag.")
	flag.Parse()

	err := host.StartHosting(*defraStorePath, *defraUrl)
	if err != nil {
		panic(fmt.Errorf("Failed to start hosting: %v", err))
	}
}
