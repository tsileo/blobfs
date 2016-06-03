package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	// fmt.Fprintf(os.Stderr, "  %s NAME MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	// hostPtr := flag.String("host", "", "remote host, default to http://localhost:8050")
	// loglevelPtr := flag.String("loglevel", "info", "logging level (debug|info|warn|crit)")
	// immutablePtr := flag.Bool("immutable", false, "make the filesystem immutable")
	// hostnamePtr := flag.String("hostname", "", "default to system hostname")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	cmd := flag.Arg(0)
	// p, err := os.Getwd()
	// if err != nil {
	// 	panic(err)
	// }
	u, err := ioutil.ReadFile(".blobfs_url")
	if err != nil {
		panic(err)
	}
	url := string(u)
	switch cmd {
	case "commit":
		if err := Commit(url); err != nil {
			panic(err)
		}
	case "prune", "checkout", "status", "log":
		fmt.Printf("Not implemented yet")
	default:
		fmt.Printf("unknown cmd %v", cmd)
	}
}

func Commit(u string) error {
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", u, "/sync"), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode == 204 {
		return nil
	}
	return fmt.Errorf("http %d", resp.StatusCode)
}
