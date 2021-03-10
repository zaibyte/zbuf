package v1

import (
	"context"
	"io/ioutil"
	"os"

	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/extent"
)

func createTestExtenter(cfg *Config) (ext *Extenter, err error) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		return nil, err
	}

	c := makeTestCreator(cfg)

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}
