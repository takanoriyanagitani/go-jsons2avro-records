package util

import (
	"context"
)

type IO[T any] func(context.Context) (T, error)

func Bind[T, U any](
	i IO[T],
	f func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}

type Void struct{}

var Empty Void = struct{}{}

func Lift[T, U any](
	f func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return f(t)
		}
	}
}
