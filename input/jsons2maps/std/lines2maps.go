package jsons2maps

import (
	"encoding/json"
	"iter"
)

func JsonLinesToMaps(
	lines iter.Seq2[[]byte, error],
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var buf map[string]any
		for line, e := range lines {
			if nil == e {
				e = json.Unmarshal(line, &buf)
			}

			if !yield(buf, e) {
				return
			}
		}
	}
}
