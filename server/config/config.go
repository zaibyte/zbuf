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

	"g.tesamc.com/IT/zbuf/extent"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/typeutil"
	"g.tesamc.com/IT/zaipkg/xio/sched"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
)

// Config is the ZBuf server configuration.
type Config struct {
	App app.Config `toml:"app"`

	// Object server listen address.
	ObjSrvAddr string `toml:"obj_srv_addr"`
	// ZBuf data root path.
	DataRoot string `toml:"data_root"`

	Scheduler sched.Config `toml:"scheduler"`

	ExtV1Config v1.Config `toml:"ext_v1_config"`
	ExtV2Config v1.Config `toml:"ext_v2_config"`
	ExtV3Config v1.Config `toml:"ext_v3_config"`
	ExtV4Config v1.Config `toml:"ext_v4_config"`
	ExtV5Config v1.Config `toml:"ext_v5_config"`

	// GCDuration is the duration between two GCs,
	// each disk will have a goroutine to do the GC job.
	GCDuration typeutil.Duration `toml:"gc_duration"`

	ZaiConfig zai.Config `toml:"zai_config"`

	// TODO in present, we only set full when disk used >= size.
	// disk won't have new extent if Keeper server think there is no enough space for making new extent.
	// DiskReservedRatio is the capacity ratio that won't make any extent anymore.
	DiskReservedRatio float64 `toml:"disk_reserved_ratio"`
	// Development mode, for testing.
	Development bool `toml:"development"`
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
	// So the block may finish before notice it, the Go Process will be wasted in this situation,
	// That's why we need more process
	// (I found this trick from this discussion: https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion)
	DefaultGOMAXPROCS = 256
)

func (c *Config) Adjust() {

	config.Adjust(&c.App.ServerAddr, DefaultOpSrvAddr)
	config.Adjust(&c.ObjSrvAddr, DefaultObjSrvAddr)

	config.Adjust(&c.DataRoot, DefaultDataRoot)

	config.Adjust(&c.App.GOMAXPROCS, DefaultGOMAXPROCS)
	runtime.GOMAXPROCS(c.App.GOMAXPROCS) // TODO maybe 512 if you got lots of cores and NVMe SSD

	config.Adjust(&c.ZaiConfig.KeeperConfig.ClusterID, c.App.KeeperClusterID)
	config.Adjust(&c.ZaiConfig.KeeperConfig.InstanceID, c.App.InstanceID)

	config.Adjust(&c.ExtV1Config.SegmentSize, extent.DefaultV1SegmentSize)
	config.Adjust(&c.ExtV2Config.SegmentSize, extent.DefaultV2SegmentSize)
	config.Adjust(&c.ExtV3Config.SegmentSize, extent.DefaultV3SegmentSize)
	config.Adjust(&c.ExtV4Config.SegmentSize, extent.DefaultV4SegmentSize)
	config.Adjust(&c.ExtV5Config.SegmentSize, extent.DefaultV5SegmentSize)
}
