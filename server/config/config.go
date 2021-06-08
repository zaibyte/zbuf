/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"runtime"

	"g.tesamc.com/IT/zaipkg/xio/sched"

	"g.tesamc.com/IT/zaipkg/typeutil"

	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zaipkg/app"
)

// Config is the ZBuf server configuration.
type Config struct {
	App app.Config `toml:"app"`

	// Object server listen address.
	ObjSrvAddr string `toml:"obj_srv_addr"`
	// ZBuf data root path.
	DataRoot string `toml:"data_root"`

	Develop DevConfig `toml:"develop"`

	// DiskWeights: disk_id: weight.
	DiskWeights map[uint32]float64 `toml:"weights"`

	Scheduler sched.Config `toml:"scheduler"`

	ExtV1Config v1.Config `toml:"ext_v_1_config"`

	// GCDuration is the duration between two GCs,
	// each disk will have a goroutine to do the GC job.
	GCDuration typeutil.Duration `toml:"gc_duration"`

	// Default protocol is TCP,
	// if true, using UDP.
	UDPEnabled bool `toml:"udp_enabled"`
}

// DevConfig is the configs only used in development, not service for production.
type DevConfig struct {
	// Development indicates Server is in development state or not,
	// we need to set it true for testing sometimes.
	Development bool `toml:"development"`
}

const (
	DefaultOpSrvAddr  = "0.0.0.0:8881"
	DefaultObjSrvAddr = "0.0.0.0:8882"
	DefaultDataRoot   = "/zai/zbuf/data"

	// DefaultGOMAXPROCS is the default settings of GOMAXPROCS.
	// Store GOMAXPROCS bigger for these reasons:
	//
	// 1. SSD is superb, and assume ZBuf runs on a server with multi-SSD, so there is a problem:
	// SSD's latency is very low, but it will take 20μs-10ms to find a thread blocked in Go.
	// So the block may finish before notice it, the GO Process will be wasted in this situation,
	// That's why we need more process
	// (I found this trick from this discussion: https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion)
	DefaultGOMAXPROCS = 256
)

func (c *Config) Adjust() {

	config.Adjust(&c.App.HTTPServerAddr, DefaultOpSrvAddr)
	config.Adjust(&c.ObjSrvAddr, DefaultObjSrvAddr)

	config.Adjust(&c.DataRoot, DefaultDataRoot)

	config.Adjust(&c.App.GOMAXPROCS, DefaultGOMAXPROCS)
	runtime.GOMAXPROCS(c.App.GOMAXPROCS) // TODO maybe 512 if you got lots of cores and NVMe SSD
}
