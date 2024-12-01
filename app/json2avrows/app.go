package app

import (
	"iter"

	util "github.com/takanoriyanagitani/go-jsons2avro-records/util"
)

type App struct {
	JsonMaps                  util.IO[iter.Seq2[map[string]any, error]]
	MapsToAvroRecordsToOutput func(
		iter.Seq2[map[string]any, error],
	) util.IO[util.Void]
}

func (a App) ToMapsToAvroRowsToOutput() util.IO[util.Void] {
	return util.Bind(
		a.JsonMaps,
		a.MapsToAvroRecordsToOutput,
	)
}
