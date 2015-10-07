package root

import "encoding/json"

var Hostname = ""

type Root struct {
	Hostname string `json:"hostname"`
	Ref      string `json:"ref"`
}

func New(ref string) *Root {
	return &Root{
		Ref:      ref,
		Hostname: Hostname,
	}
}

func NewFromJSON(data []byte) (*Root, error) {
	root := &Root{}
	if err := json.Unmarshal(data, root); err != nil {
		return nil, err
	}
	return root, nil
}
