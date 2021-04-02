package v1

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestObjHeaderMarshal(t *testing.T) {

	rand.Seed(tsc.UnixNano())

	p := make([]byte, objHeaderSize)
	for i := 0; i < 1024; i++ {

		oid := rand.Uint64()
		if oid == 0 {
			oid = 1
		}

		h := &objHeader{
			oid:    oid,
			grains: rand.Uint32(),
			cycle:  rand.Uint32(),
			extID:  rand.Uint32(),
			offset: rand.Int63(),
		}

		h.marshalTo(p)

		act := new(objHeader)
		err := act.unmarshal(p)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, h, act)
	}

	h := &objHeader{oid: 0}
	h.marshalTo(p)
	act := new(objHeader)
	err := act.unmarshal(p)
	if !errors.Is(err, ErrUnwrittenSeg) {
		t.Fatal("should be ErrUnwrittenSeg")
	}

	oid := rand.Uint64()
	if oid == 0 {
		oid = 1
	}

	h = &objHeader{
		oid:    oid,
		grains: rand.Uint32(),
		cycle:  rand.Uint32(),
		extID:  rand.Uint32(),
		offset: rand.Int63(),
	}

	h.marshalTo(p)

	binary.LittleEndian.PutUint64(p, oid+1)

	act = new(objHeader)
	err = act.unmarshal(p)
	if !errors.Is(err, orpc.ErrChecksumMismatch) {
		t.Fatal("checksum should be mismatched")
	}

	binary.LittleEndian.PutUint64(p, oid)
	binary.LittleEndian.PutUint64(p[objHeaderMagicNumberOffset:], objHeaderMagicNumber+1)

	err = act.unmarshal(p)
	if !errors.Is(err, ErrIllegalObjHeader) {
		t.Fatal("object header should be illegal", err)
	}
}
