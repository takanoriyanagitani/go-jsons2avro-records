package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	util "github.com/takanoriyanagitani/go-jsons2avro-records/util"

	js "github.com/takanoriyanagitani/go-jsons2avro-records/input/jsons2maps/std"
	ir "github.com/takanoriyanagitani/go-jsons2avro-records/input/read2lines"

	mh "github.com/takanoriyanagitani/go-jsons2avro-records/output/maps2avro/hamba"

	ap "github.com/takanoriyanagitani/go-jsons2avro-records/app/json2avrows"
)

func GetEnvByKey(key string) util.IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		if !found {
			return "", fmt.Errorf("env var %s undefined", key)
		}
		return val, nil
	}
}

const SchemaSizeLimit int64 = 1048576

func FilenameToString(limit int64) func(string) util.IO[string] {
	return util.Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)

		return buf.String(), e
	})
}

var FilenameToStringDefault func(string) util.IO[string] = FilenameToString(
	SchemaSizeLimit,
)

var schemaFilename util.IO[string] = GetEnvByKey("ENV_SCHEMA_FILENAME")
var schemaString util.IO[string] = util.Bind(
	schemaFilename,
	FilenameToStringDefault,
)

var jsonLines util.IO[iter.Seq2[[]byte, error]] = ir.StdinToLineIterator

var jsonMaps util.IO[iter.Seq2[map[string]any, error]] = util.Bind(
	jsonLines,
	func(
		lines iter.Seq2[[]byte, error],
	) util.IO[iter.Seq2[map[string]any, error]] {
		return func(
			_ context.Context,
		) (iter.Seq2[map[string]any, error], error) {
			return js.JsonLinesToMaps(lines), nil
		}
	},
)

var app util.IO[ap.App] = util.Bind(
	schemaString,
	func(schema string) util.IO[ap.App] {
		return func(_ context.Context) (ap.App, error) {
			return ap.App{
				JsonMaps:                  jsonMaps,
				MapsToAvroRecordsToOutput: mh.SchemaStringToConverter(schema),
			}, nil
		}
	},
)

var stdin2jsons2avro2records2stdout util.IO[util.Void] = util.Bind(
	app,
	func(a ap.App) util.IO[util.Void] { return a.ToMapsToAvroRowsToOutput() },
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, e := stdin2jsons2avro2records2stdout(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
