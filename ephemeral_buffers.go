/**
 * Copyright (c) 2021, Xerra Earth Observation Institute
 * All rights reserved. Use is subject to License terms.
 * See LICENSE.TXT in the root directory of this source tree.
 */

package ephemeral_buffers

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Buffer is a wrapper around bytes.Buffer{} and as such implements all the things
// that bytes.Buffer{} does, including the io.Writer interface.
// It is not intended to be used directly, but Acquire()d from a Pool.
type Buffer struct {
	bytes.Buffer
	pool         *Pool
	index        int
	origSize     int
	acquiredAt   time.Time
	tag          string
}

// Release a Buffer back to the Pool it was Acquire()d from.
func (eb *Buffer) Release() {
	if eb.pool == nil {
		return
	}

	// check size
	if eb.Cap() > eb.origSize {
		log.Ctx(eb.pool.ctx).Warn().Msgf("Buffer with tag %s allocated %d bytes (%d requested)",
			eb.tag, eb.Cap(), eb.pool.size)

		eb.origSize = eb.Cap()
	}

	eb.pool.lock.Lock()

	eb.pool.buffersInUse[eb.index] = nil

	eb.pool.lock.Unlock()

	eb.index = -1
	eb.tag = ""
	eb.Reset()

	eb.pool.buffersAvailable <- eb
}

// Pool implements a pool of buffers. Create a new pool using the NewPool() function.
type Pool struct {
	size             int
	count            int
	ctx              context.Context
	lock             sync.Mutex
	buffersAvailable chan *Buffer
	buffersInUse     []*Buffer
}

// All Buffers should be released as soon as they are not needed.
func (p *Pool) Acquire(tag string) *Buffer {
	if p.size == 0 || p.count == 0 {
		return nil
	}

	eb := <-p.buffersAvailable

	p.lock.Lock()

	for i := range p.buffersInUse {
		if p.buffersInUse[i] == nil {
			p.buffersInUse[i] = eb
			eb.index = i

			break
		}
	}

	p.lock.Unlock()

	eb.tag = tag
	eb.acquiredAt = time.Now()

	return eb
}

// Free waits for all Buffers in the Pool to be Release()d and then frees
// the memory used by the Pool. Technically it just releases
func (p *Pool) Free() {
	if p.size == 0 || p.count == 0 {
		return
	}

	count := p.count

	p.lock.Lock()

	// causes the poolMonitor to exit
	p.count = 0

	p.lock.Unlock()

	// read all buffersAvailable and discard them
	for i := 0; i < count; i++ {
		<-p.buffersAvailable
	}

	p.buffersInUse = nil
}

// BuffersAvailable() returns the number of unused Buffers in the Pool.
func (p *Pool) BuffersAvailable() int {
	if p.size == 0 || p.count == 0 {
		return 0
	}

	i := 0

	p.lock.Lock()

	for _, b := range p.buffersInUse {
		if b == nil {
			i++
		}
	}

	p.lock.Unlock()

	return i
}

// NewPool creates a new Pool of Buffers.
// It creates count Buffers, of size bytes each.
// It is possible to write more than the allocate bytes into the buffers, however
// a warning will be issued when the buffer is released.
// The context is used for logging.
// Use Pool.Free() to dispose of the buffer when no longer needed.
func NewPool(ctx context.Context, count, size int) *Pool {
	if count <= 0 || size <= 0 {
		log.Ctx(ctx).Error().Msgf("Invalid arguments in call to NewPool")

		return nil
	}

	p := Pool{size: size, count: count, ctx: ctx}

	p.buffersInUse = make([]*Buffer, count)
	p.buffersAvailable = make(chan *Buffer, count)

	for i := 0; i < count; i++ {
		buf := bytes.Buffer{}
		buf.Grow(size)
		ebuf := Buffer{Buffer: buf, pool: &p, origSize: size}
		p.buffersAvailable <-&ebuf
	}

	go p.poolMonitor()

	return &p
}

// poolMonitor checks that buffers are released in a timely fashion.
func (p *Pool) poolMonitor() {
	for {
		p.lock.Lock()

		if p.count == 0 {
			p.lock.Unlock()

			log.Ctx(p.ctx).Debug().Msgf("Buffers.poolMonitor exiting")

			return
		}

		now := time.Now()

		for _, eb := range p.buffersInUse {
			if eb != nil {
				if eb.acquiredAt.Add(100 * time.Millisecond).Before(now) {
					log.Ctx(p.ctx).Warn().Msgf("Buffer with tag %s held "+
						"for %v", eb.tag, time.Since(eb.acquiredAt))
				}
			}
		}

		p.lock.Unlock()

		time.Sleep(1 * time.Second)
	}
}
