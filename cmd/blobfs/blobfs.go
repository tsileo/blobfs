package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type CommitLog struct {
	T       string `json:"t"`
	Version int    `json:"version"`
	Comment string `json:"comment"`
}

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
	commentPtr := flag.String("comment", "", "optional commit comment")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() < 1 {
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
		fmt.Printf("comment: %v", *commentPtr)
		if err := Commit(url, *commentPtr); err != nil {
			panic(err)
		}
	case "log":
		if err := Log(url); err != nil {
			panic(err)
		}
	case "prune", "checkout", "status":
		fmt.Printf("Not implemented yet")
	default:
		fmt.Printf("unknown cmd %v", cmd)
	}
}

func Log(u string) error {
	request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", u, "/log"), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	logs := []*CommitLog{}
	if err := json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		return err
	}
	// TODO(tsileo): add a * to the current checked out version, look how git is doing for branch
	for _, log := range logs {
		fmt.Printf("%s  %v\t%s\n", log.T, log.Version, log.Comment)
	}
	return nil
}

func Commit(u, msg string) error {
	body, err := json.Marshal(map[string]interface{}{"comment": msg})
	if err != nil {
		return err
	}
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", u, "/sync"), bytes.NewReader(body))
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
