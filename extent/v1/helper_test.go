package v1

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/xtest"

	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
)

func TestMain(m *testing.M) {

	flag.Parse()

	os.Exit(m.Run())
}

// Reference:
//
// n: 0.50MB, delta: 16s, cost: 0.04
// n: 0.50MB, delta: 15s, cost: 0.08
// n: 0.50MB, delta: 14s, cost: 0.15
// n: 0.50MB, delta: 13s, cost: 0.28
// n: 0.50MB, delta: 12s, cost: 0.52
// n: 0.50MB, delta: 11s, cost: 0.96
// n: 0.50MB, delta: 10s, cost: 1.78
// n: 0.50MB, delta: 9s, cost: 3.30
// n: 0.50MB, delta: 8s, cost: 6.12
// n: 0.50MB, delta: 7s, cost: 11.35
// n: 0.50MB, delta: 6s, cost: 21.05
// n: 0.50MB, delta: 5s, cost: 39.06
// n: 0.50MB, delta: 4s, cost: 72.46
// n: 0.50MB, delta: 3s, cost: 134.42
// n: 0.50MB, delta: 2s, cost: 249.38
// n: 0.50MB, delta: 1s, cost: 462.65
// n: 1.00MB, delta: 16s, cost: 0.07
// n: 1.00MB, delta: 15s, cost: 0.12
// n: 1.00MB, delta: 14s, cost: 0.23
// n: 1.00MB, delta: 13s, cost: 0.43
// n: 1.00MB, delta: 12s, cost: 0.79
// n: 1.00MB, delta: 11s, cost: 1.47
// n: 1.00MB, delta: 10s, cost: 2.73
// n: 1.00MB, delta: 9s, cost: 5.06
// n: 1.00MB, delta: 8s, cost: 9.38
// n: 1.00MB, delta: 7s, cost: 17.40
// n: 1.00MB, delta: 6s, cost: 32.28
// n: 1.00MB, delta: 5s, cost: 59.89
// n: 1.00MB, delta: 4s, cost: 111.11
// n: 1.00MB, delta: 3s, cost: 206.13
// n: 1.00MB, delta: 2s, cost: 382.42
// n: 1.00MB, delta: 1s, cost: 709.48
// n: 2.00MB, delta: 16s, cost: 0.10
// n: 2.00MB, delta: 15s, cost: 0.19
// n: 2.00MB, delta: 14s, cost: 0.35
// n: 2.00MB, delta: 13s, cost: 0.65
// n: 2.00MB, delta: 12s, cost: 1.21
// n: 2.00MB, delta: 11s, cost: 2.25
// n: 2.00MB, delta: 10s, cost: 4.18
// n: 2.00MB, delta: 9s, cost: 7.76
// n: 2.00MB, delta: 8s, cost: 14.39
// n: 2.00MB, delta: 7s, cost: 26.69
// n: 2.00MB, delta: 6s, cost: 49.52
// n: 2.00MB, delta: 5s, cost: 91.87
// n: 2.00MB, delta: 4s, cost: 170.44
// n: 2.00MB, delta: 3s, cost: 316.20
// n: 2.00MB, delta: 2s, cost: 586.61
// n: 2.00MB, delta: 1s, cost: 1088.29
// n: 4.00MB, delta: 16s, cost: 0.16
// n: 4.00MB, delta: 15s, cost: 0.29
// n: 4.00MB, delta: 14s, cost: 0.54
// n: 4.00MB, delta: 13s, cost: 1.00
// n: 4.00MB, delta: 12s, cost: 1.86
// n: 4.00MB, delta: 11s, cost: 3.46
// n: 4.00MB, delta: 10s, cost: 6.41
// n: 4.00MB, delta: 9s, cost: 11.90
// n: 4.00MB, delta: 8s, cost: 22.07
// n: 4.00MB, delta: 7s, cost: 40.95
// n: 4.00MB, delta: 6s, cost: 75.97
// n: 4.00MB, delta: 5s, cost: 140.95
// n: 4.00MB, delta: 4s, cost: 261.49
// n: 4.00MB, delta: 3s, cost: 485.11
// n: 4.00MB, delta: 2s, cost: 899.99
// n: 4.00MB, delta: 1s, cost: 1669.67
// n: 8.00MB, delta: 16s, cost: 0.24
// n: 8.00MB, delta: 15s, cost: 0.45
// n: 8.00MB, delta: 14s, cost: 0.83
// n: 8.00MB, delta: 13s, cost: 1.54
// n: 8.00MB, delta: 12s, cost: 2.86
// n: 8.00MB, delta: 11s, cost: 5.30
// n: 8.00MB, delta: 10s, cost: 9.84
// n: 8.00MB, delta: 9s, cost: 18.26
// n: 8.00MB, delta: 8s, cost: 33.87
// n: 8.00MB, delta: 7s, cost: 62.84
// n: 8.00MB, delta: 6s, cost: 116.57
// n: 8.00MB, delta: 5s, cost: 216.27
// n: 8.00MB, delta: 4s, cost: 401.22
// n: 8.00MB, delta: 3s, cost: 744.36
// n: 8.00MB, delta: 2s, cost: 1380.94
// n: 8.00MB, delta: 1s, cost: 2561.94
// n: 16.00MB, delta: 16s, cost: 0.37
// n: 16.00MB, delta: 15s, cost: 0.69
// n: 16.00MB, delta: 14s, cost: 1.27
// n: 16.00MB, delta: 13s, cost: 2.36
// n: 16.00MB, delta: 12s, cost: 4.39
// n: 16.00MB, delta: 11s, cost: 8.14
// n: 16.00MB, delta: 10s, cost: 15.10
// n: 16.00MB, delta: 9s, cost: 28.01
// n: 16.00MB, delta: 8s, cost: 51.97
// n: 16.00MB, delta: 7s, cost: 96.42
// n: 16.00MB, delta: 6s, cost: 178.88
// n: 16.00MB, delta: 5s, cost: 331.87
// n: 16.00MB, delta: 4s, cost: 615.69
// n: 16.00MB, delta: 3s, cost: 1142.23
// n: 16.00MB, delta: 2s, cost: 2119.08
// n: 16.00MB, delta: 1s, cost: 3931.35
// n: 32.00MB, delta: 16s, cost: 0.57
// n: 32.00MB, delta: 15s, cost: 1.05
// n: 32.00MB, delta: 14s, cost: 1.96
// n: 32.00MB, delta: 13s, cost: 3.63
// n: 32.00MB, delta: 12s, cost: 6.73
// n: 32.00MB, delta: 11s, cost: 12.49
// n: 32.00MB, delta: 10s, cost: 23.17
// n: 32.00MB, delta: 9s, cost: 42.99
// n: 32.00MB, delta: 8s, cost: 79.76
// n: 32.00MB, delta: 7s, cost: 147.97
// n: 32.00MB, delta: 6s, cost: 274.52
// n: 32.00MB, delta: 5s, cost: 509.29
// n: 32.00MB, delta: 4s, cost: 944.83
// n: 32.00MB, delta: 3s, cost: 1752.87
// n: 32.00MB, delta: 2s, cost: 3251.94
// n: 32.00MB, delta: 1s, cost: 6033.05
// n: 64.00MB, delta: 16s, cost: 0.87
// n: 64.00MB, delta: 15s, cost: 1.62
// n: 64.00MB, delta: 14s, cost: 3.00
// n: 64.00MB, delta: 13s, cost: 5.57
// n: 64.00MB, delta: 12s, cost: 10.33
// n: 64.00MB, delta: 11s, cost: 19.17
// n: 64.00MB, delta: 10s, cost: 35.56
// n: 64.00MB, delta: 9s, cost: 65.98
// n: 64.00MB, delta: 8s, cost: 122.40
// n: 64.00MB, delta: 7s, cost: 227.08
// n: 64.00MB, delta: 6s, cost: 421.29
// n: 64.00MB, delta: 5s, cost: 781.57
// n: 64.00MB, delta: 4s, cost: 1449.99
// n: 64.00MB, delta: 3s, cost: 2690.04
// n: 64.00MB, delta: 2s, cost: 4990.60
// n: 64.00MB, delta: 1s, cost: 9258.63
// n: 128.00MB, delta: 16s, cost: 1.34
// n: 128.00MB, delta: 15s, cost: 2.48
// n: 128.00MB, delta: 14s, cost: 4.61
// n: 128.00MB, delta: 13s, cost: 8.55
// n: 128.00MB, delta: 12s, cost: 15.86
// n: 128.00MB, delta: 11s, cost: 29.42
// n: 128.00MB, delta: 10s, cost: 54.58
// n: 128.00MB, delta: 9s, cost: 101.25
// n: 128.00MB, delta: 8s, cost: 187.85
// n: 128.00MB, delta: 7s, cost: 348.50
// n: 128.00MB, delta: 6s, cost: 646.54
// n: 128.00MB, delta: 5s, cost: 1199.47
// n: 128.00MB, delta: 4s, cost: 2225.27
// n: 128.00MB, delta: 3s, cost: 4128.36
// n: 128.00MB, delta: 2s, cost: 7658.99
// n: 128.00MB, delta: 1s, cost: 14209.06
// n: 256.00MB, delta: 16s, cost: 2.05
// n: 256.00MB, delta: 15s, cost: 3.81
// n: 256.00MB, delta: 14s, cost: 7.07
// n: 256.00MB, delta: 13s, cost: 13.12
// n: 256.00MB, delta: 12s, cost: 24.34
// n: 256.00MB, delta: 11s, cost: 45.15
// n: 256.00MB, delta: 10s, cost: 83.76
// n: 256.00MB, delta: 9s, cost: 155.40
// n: 256.00MB, delta: 8s, cost: 288.29
// n: 256.00MB, delta: 7s, cost: 534.84
// n: 256.00MB, delta: 6s, cost: 992.25
// n: 256.00MB, delta: 5s, cost: 1840.83
// n: 256.00MB, delta: 4s, cost: 3415.14
// n: 256.00MB, delta: 3s, cost: 6335.82
// n: 256.00MB, delta: 2s, cost: 11754.29
// n: 256.00MB, delta: 1s, cost: 21806.73
func TestCalcSnapCost2(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("we already got data, run it again unless you've changed the algorithm")
	}

	var start int64 = phyaddr.MinCap * 8
	var end int64 = phyaddr.MaxCap * 8
	for n := start; n <= end; n *= 2 {
		var last int64 = 0
		var now int64 = 16 * int64(time.Second)
		rets := make(map[int64]float64)
		for l := last; l < now; l += int64(time.Second) {
			c := calcSnapCost(n, l, now)
			rets[now-l] = c
			fmt.Printf("n: %.2fMB, delta: %ds, cost: %.2f\n", float64(n)/1024/1024, (now-l)/int64(time.Second), c)
		}
	}
}
