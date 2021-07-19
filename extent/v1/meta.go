package v1

import (
	"g.tesamc.com/IT/zaipkg/extutil"
	"g.tesamc.com/IT/zproto/pkg/metapb"
	"github.com/templexxx/tsc"

	"github.com/gogo/protobuf/proto"
)

// UpdateMeta updates meta in Extenter.
// It's used for handling heartbeat response,
// only ext.state & clone job (nil -> new or new -> nil) & clone job's oids_oid could be changed by heartbeat.
func (e *Extenter) UpdateMeta(m *metapb.Extent) {

	// meta could not be nil, after Extenter starting.
	e.rwMutex.Lock()
	if m.State != e.meta.State {
		extutil.SetState(e.meta, m.State)
	}

	if m.CloneJob == nil {
		if e.meta.CloneJob != nil { // Must be done.
			e.meta.CloneJob = nil
		}
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

// GetMeta returns Extenter's meta, clone it avoiding race.
// For heartbeat request.
func (e *Extenter) GetMeta() *metapb.Extent {

	e.rwMutex.RLock()
	ext := proto.Clone(e.meta).(*metapb.Extent)
	e.rwMutex.RUnlock()
	// Set lastUpdate when get, we don't need accurate lastUpdate.
	// It would be annoyed if we modify it in every changes.
	ext.LastUpdate = tsc.UnixNano()
	return ext
}
