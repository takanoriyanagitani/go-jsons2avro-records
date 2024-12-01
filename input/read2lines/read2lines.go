package read2lines

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	util "github.com/takanoriyanagitani/go-jsons2avro-records/util"
)

func ReaderToLines(r io.Reader) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		var s *bufio.Scanner = bufio.NewScanner(r)
		for s.Scan() {
			var line []byte = s.Bytes()
			if !yield(line, nil) {
				return
			}
		}
	}
}

func StdinToLines(_ context.Context) (iter.Seq2[[]byte, error], error) {
	return ReaderToLines(os.Stdin), nil
}

var StdinToLineIterator util.IO[iter.Seq2[[]byte, error]] = StdinToLines
