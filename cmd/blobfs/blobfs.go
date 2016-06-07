package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/AlekSi/xattr"
	"github.com/fatih/color"
)

var (
	LogStaging = "STAGING"
	LogLatest  = "LATEST"
)

var (
	yellow     = color.New(color.FgYellow).SprintFunc()
	yellowBold = color.New(color.FgYellow, color.Bold).SprintFunc()
	bold       = color.New(color.Bold).SprintFunc()
)

type CommitLog struct {
	T       string `json:"t"`
	Ref     string `json:"ref"`
	Comment string `json:"comment"`
	Current bool   `json:"current"`
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s COMMAND\n", os.Args[0])
	flag.PrintDefaults()
}

func isPublic(path string) (bool, error) {
	res, err := xattr.Get(path, "public")
	if err != nil {
		if !xattr.IsNotExist(err) {
			return false, err
		}
	} else {
		if string(res) == "1" {
			return true, nil
		}
	}
	return false, nil
}

func main() {
	commentPtr := flag.String("comment", "", "optional commit comment")
	publicPtr := flag.Bool("public", false, "share the node publicly (default to semi-private)")
	// shareTTLPtr := flag.String("share-ttl", "1h", "TTL for the semi-private sharing linl (default to 1h)")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() < 1 {
		Usage()
		os.Exit(2)
	}
	cmd := flag.Arg(0)
	u, err := ioutil.ReadFile(".blobfs_url")
	// TODO(tsileo): do the same for bash
	if cmd == "__ps1_bash" {
		if os.IsNotExist(err) {
			fmt.Printf("")
			return
		}
		request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", u, "/ref"), nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			return
		}
		if resp.StatusCode != 200 {
			return
		}
		rr := &RefResp{}
		if err := json.NewDecoder(resp.Body).Decode(rr); err != nil {
			return
		}
		fmt.Printf("%s:(%s) ", bold("blobfs"), yellow(rr.Ref))
		return
	}
	if cmd == "__ps1_zsh" {
		if os.IsNotExist(err) {
			fmt.Printf("")
			return
		}
		// TODO(tsileo): a getRef
		request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", u, "/ref"), nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			return
		}
		if resp.StatusCode != 200 {
			return
		}
		rr := &RefResp{}
		if err := json.NewDecoder(resp.Body).Decode(rr); err != nil {
			return
		}
		fmt.Print("%Bblobfs%b:(%{\033[33m%}" + rr.Ref + "%{\033[0m%}) ")
		return
	}
	if err != nil {
		panic(err)
	}
	url := string(u)
	switch cmd {
	case "commit":
		if err := Commit(url, *commentPtr); err != nil {
			panic(err)
		}
	case "checkout":
		if err := Checkout(url, flag.Arg(1)); err != nil {
			panic(err)
		}
	case "history", "log":
		if err := Log(url); err != nil {
			panic(err)
		}
	case "status":
		if err := Status(url); err != nil {
			panic(err)
		}
	case "share":
		path := "."
		if flag.NArg() == 2 {
			path = flag.Arg(1)
		}
		public, err := isPublic(path)
		if err != nil {
			panic(err)
		}
		fmt.Printf("public:%s\n", public)

		// Share in "public" mode
		if *publicPtr {

			if public {
				burl, err := xattr.Get(path, "url")
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", burl)
			} else {
				if err := xattr.Set(path, "public", []byte("1")); err != nil {
					panic(err)
				}
				burl, err := xattr.Get(path, "url")
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s\n", burl)
			}
			fmt.Printf("You still need to commit for the file to become available.")
			return

		}

		// Share in semi-private mode (e.g. anyone with the link can access it)
		// XXX(tsileo): call the API to get a bewit signed link

	case "unshare":
		path := "."
		if flag.NArg() == 2 {
			path = flag.Arg(1)
		}
		public, err := isPublic(path)
		if err != nil {
			panic(err)
		}
		fmt.Printf("public:%s\n", public)
		if !public {
			// XXX(tsileo): color in red?
			fmt.Printf("You can only unshare public nodes")
			// TODO(tsileo): return with error code
			return
		}

		if err := xattr.Set(path, "public", []byte("0")); err != nil {
			panic(err)
		}
		fmt.Printf("You still need to commit for the file to become unavailable.")
	case "prune", "public": // XXX(tsileo): find a better name than `public` for listing public nodes
		fmt.Printf("Not implemented yet")
	default:
		fmt.Printf("unknown cmd %v", cmd)
	}
}

type StatusResp struct {
	Added    []string `json:"added"`
	Modified []string `json:"modified"`
	Deleted  []string `json:"deleted"`
}

type RefResp struct {
	Ref string `json:"ref"`
}

func buildStatusIndex(in []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, path := range in {
		out[path] = struct{}{}
	}
	return out
}

func Status(u string) error {
	request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", u, "/status"), nil)
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
	if resp.StatusCode != 200 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	sr := &StatusResp{}
	if err := json.NewDecoder(resp.Body).Decode(sr); err != nil {
		return err
	}
	deletedIndex := buildStatusIndex(sr.Deleted)
	modifiedIndex := buildStatusIndex(sr.Modified)
	addedIndex := buildStatusIndex(sr.Added)
	paths := []string{}
	for _, p := range sr.Added {
		paths = append(paths, p)
	}
	for _, p := range sr.Deleted {
		paths = append(paths, p)
	}
	for _, p := range sr.Modified {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		var letter string
		if _, ok := addedIndex[p]; ok {
			letter = "A"
		}
		if _, ok := modifiedIndex[p]; ok {
			letter = "M"
		}
		if _, ok := deletedIndex[p]; ok {
			letter = "D"
		}
		fmt.Printf("%s  %s\n", yellow(letter), p)
	}
	return nil
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
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	for _, log := range logs {
		if log.Current {
			fmt.Fprintf(w, "* %s\t%s\t%s\n", yellow(log.Ref), log.T, log.Comment)
		} else {
			fmt.Fprintf(w, "  %s\t%s\t%s\n", log.Ref, log.T, log.Comment)
		}
	}
	w.Flush()
	return nil
}

func Checkout(u, ref string) error {
	body, err := json.Marshal(map[string]interface{}{"ref": ref})
	if err != nil {
		return err
	}
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", u, "/checkout"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 {
		return nil
	}
	return fmt.Errorf("http %d", resp.StatusCode)
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
