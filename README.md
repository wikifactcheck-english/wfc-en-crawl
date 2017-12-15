# Install/run
Install `go`. Put this directory in your `GOPATH` under `github.com/mammothbane`.

Generate reference index (expects `refdata` and `out` to exist):
```shell
go run cmd/refidx/refidx.go
```

Download references:
```shell
go run cmd/refdl/refd.go
```

If you want to parallelize more heavily, split your index `index.txt` into parts into the `indices` diretory, then run
`run_parts.py` and a separate go process will download all the parts.
