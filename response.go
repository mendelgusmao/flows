package flows

import (
	"encoding/json"
)

type Response struct {
	Topic   string
	Content []byte
}

func (r Response) TryJSON(v interface{}) bool {
	return json.Unmarshal(r.Content, v) == nil
}
