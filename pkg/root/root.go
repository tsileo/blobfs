package root

import "encoding/json"

var Hostname = ""

// XXX(tsileo): rename to commit? keep the hostname?
type Root struct {
	Hostname string `json:"hostname"`
	Ref      string `json:"ref"`
	Comment  string `json:"comment"`
	Version  int    `json:"-"`
}

func New(ref string, version int) *Root {
	return &Root{
		Ref:      ref,
		Hostname: Hostname,
		Version:  version,
	}
}

func (r *Root) JSON() ([]byte, error) {
	return json.Marshal(r)
}

func NewFromJSON(data []byte, version int) (*Root, error) {
	root := &Root{Version: version}
	if err := json.Unmarshal(data, root); err != nil {
		return nil, err
	}
	return root, nil
}
