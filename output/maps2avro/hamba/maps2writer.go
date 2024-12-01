package maps2avro

import (
	"context"
	"errors"
	"io"
	"iter"
	"log"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	util "github.com/takanoriyanagitani/go-jsons2avro-records/util"
)

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
) error {
	enc, e := ho.NewEncoderWithSchema(s, w)
	if nil != e {
		return e
	}
	defer func() {
		ef := enc.Flush()
		ec := enc.Close()
		e := errors.Join(ef, ec)
		if nil != e {
			log.Printf("%v\n", e)
		}
	}()

	for mp, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		e = enc.Encode(mp)
		if nil != e {
			return e
		}
	}
	return nil
}

func SchemaStringToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s string,
) error {
	schema, e := ha.Parse(s)
	if nil != e {
		return e
	}
	return MapsToWriter(ctx, m, w, schema)
}

func SchemaStringToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	s string,
) error {
	return SchemaStringToWriter(ctx, m, os.Stdout, s)
}

func SchemaStringToConverter(
	schema string,
) func(iter.Seq2[map[string]any, error]) util.IO[util.Void] {
	return func(m iter.Seq2[map[string]any, error]) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			return util.Empty, SchemaStringToStdout(ctx, m, schema)
		}
	}
}
