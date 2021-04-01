package v1

import (
	"errors"
	"io"
	"math/rand"
	"testing"

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
			segID:  uint8(rand.Intn(256)),
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
	if !errors.Is(err, io.EOF) {
		t.Fatal("should be io.EIO")
	}
}
