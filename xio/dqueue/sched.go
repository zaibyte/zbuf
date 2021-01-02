package dqueue

import (
	"context"
	"sync"
)

type Scheduler struct {
	dqueue *DiskQueue

	ctx    context.Context
	stopWg *sync.WaitGroup
}
