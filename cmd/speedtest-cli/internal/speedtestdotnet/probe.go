package speedtestdotnet

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mattn/go-isatty"
	"go.jonnrb.io/speedtest/speedtestdotnet"
	"go.jonnrb.io/speedtest/units"
)

var outTTY = false

func init() {
	outTTY = isatty.IsTerminal(uintptr(os.Stdout.Fd()))
}

func erasePrevious() {
	fmt.Print("\033[1A") // ANSI escape sequence for move one line up
	fmt.Print("\033[K")  // ANSI escape sequence for erase current line
}

func download(client *speedtestdotnet.Client, server speedtestdotnet.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), *dlTime)
	defer cancel()

	printSpeed := func(speed units.BytesPerSecond) {
		// Default return speed is in bytes.
		if *fmtBytes {
			fmt.Printf("Download speed: %v\n", speed)
		} else {
			fmt.Printf("Download speed: %v\n", speed.BitsPerSecond())
		}
	}

	var stream chan units.BytesPerSecond
	done := make(chan struct{})
	if outTTY {
		stream = make(chan units.BytesPerSecond)
		go func() {
			for speed := range stream {
				erasePrevious()
				printSpeed(speed)
			}
			close(done)
		}()
	}

	if outTTY {
		printSpeed(units.BytesPerSecond(0))
	}
	if speed, err := server.ProbeDownloadSpeed(ctx, client, stream); err != nil {
		log.Fatalf("Error probing download speed: %v", err)
	} else {
		if outTTY {
			<-done
			erasePrevious()
		}
		printSpeed(speed)
	}
}

func upload(client *speedtestdotnet.Client, server speedtestdotnet.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), *ulTime)
	defer cancel()

	printSpeed := func(speed units.BytesPerSecond) {
		// Default return speed is in bytes.
		if *fmtBytes {
			fmt.Printf("Upload speed: %v\n", speed)
		} else {
			fmt.Printf("Upload speed: %v\n", speed.BitsPerSecond())
		}
	}

	var stream chan units.BytesPerSecond
	done := make(chan struct{})
	if outTTY {
		stream = make(chan units.BytesPerSecond)
		go func() {
			for speed := range stream {
				erasePrevious()
				printSpeed(speed)
			}
			close(done)
		}()
	}

	if outTTY {
		printSpeed(units.BytesPerSecond(0))
	}
	if speed, err := server.ProbeUploadSpeed(ctx, client, stream); err != nil {
		log.Fatalf("Error probing upload speed: %v", err)
	} else {
		if outTTY {
			<-done
			erasePrevious()
		}
		printSpeed(speed)
	}
}