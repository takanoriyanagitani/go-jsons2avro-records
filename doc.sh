#!/bin/sh

port=11308
addr="localhost:${port}"

pkgsite \
	--http "${addr}"
