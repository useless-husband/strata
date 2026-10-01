//go:build !darwin

// Command syncprobe measures macOS's F_FULLFSYNC; elsewhere it only says so.
package main

import "fmt"

func main() { fmt.Println("syncprobe measures F_FULLFSYNC, which exists only on macOS") }
