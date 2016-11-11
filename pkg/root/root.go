package root

import "encoding/json"

var Hostname = ""

// XXX(tsileo): rename to commit? keep the hostname?
type Root struct {
	Hostname string `json:"hostname"`
	Ref      string `json:"ref"`
	Comment  string `json:"comment"`
}

func New(ref string) *Root {
	return &Root{
		Ref:      ref,
		Hostname: Hostname,
	}
}

func (r *Root) JSON() ([]byte, error) {
	return json.Marshal(r)
}

func NewFromJSON(data []byte) (*Root, error) {
	root := &Root{}
	if err := json.Unmarshal(data, root); err != nil {
		return nil, err
	}
	return root, nil
}
