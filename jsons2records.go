package jsons2records

import (
	"iter"
)

type JsonMap map[string]any

type JsonMaps iter.Seq2[JsonMap, error]
