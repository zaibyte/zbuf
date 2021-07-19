package v1

import (
	"g.tesamc.com/IT/zaipkg/extutil"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/gogo/protobuf/proto"
)

func (e *Extenter) UpdateMeta(m *metapb.Extent) {

	e.rwMutex.Lock()
	if m.State != e.meta.State {
		extutil.SetState(e.meta, m.State)
	}
	if m.CloneJob != nil && e.meta.CloneJob == nil {
		e.meta.CloneJob = proto.Clone(m.CloneJob).(*metapb.CloneJob)
	}
	if e.meta.CloneJob != nil && m.CloneJob != nil {
		if m.CloneJob.OidsOid != 0 && e.meta.CloneJob.OidsOid == 0 {
			e.meta.CloneJob.OidsOid = m.CloneJob.OidsOid
		}
	}
	e.rwMutex.Unlock()
}

func (e *Extenter) GetMeta() *metapb.Extent {

	e.rwMutex.RLock()
	ext := proto.Clone(e.meta).(*metapb.Extent)
	e.rwMutex.RUnlock()
	return ext
}
