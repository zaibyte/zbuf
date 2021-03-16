module g.tesamc.com/IT/zbuf

go 1.15

require (
	g.tesamc.com/IT/zai v0.0.0-20210223033300-3ff542d347f8
	g.tesamc.com/IT/zaipkg v0.0.0-20210221102003-7de4fb06bd59
	g.tesamc.com/IT/zproto v0.0.0-20210223015400-40e6b2bc1b38
	github.com/VictoriaMetrics/metrics v1.12.3
	github.com/elastic/go-hdrhistogram v0.1.0
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/julienschmidt/httprouter v1.2.0
	github.com/lni/goutils v1.2.0
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.6.1
	github.com/templexxx/cpu v0.0.8-0.20200904080713-862a179c181c
	github.com/templexxx/fnc v1.0.1
	github.com/templexxx/tsc v0.0.2-0.20201016082558-86c1143c3415
	github.com/templexxx/xorsimd v0.4.1
	github.com/willf/bitset v1.1.11 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/sys v0.0.0-20210119212857-b64e53b001e4
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
)

// TODO GitLAB proxy issues
replace (
	g.tesamc.com/IT/zai v0.0.0-20210223033300-3ff542d347f8 => ../zai
	g.tesamc.com/IT/zaipkg v0.0.0-20210221102003-7de4fb06bd59 => ../zaipkg
	g.tesamc.com/IT/zproto v0.0.0-20210223015400-40e6b2bc1b38 => ../zproto
)
