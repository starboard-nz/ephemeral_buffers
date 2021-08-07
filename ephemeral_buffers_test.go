/**
 * Copyright (c) 2021, Xerra Earth Observation Institute
 * All rights reserved. Use is subject to License terms.
 * See LICENSE.TXT in the root directory of this source tree.
 */

package ephemeral_buffers_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"gitlab.com/xerra/common/ephemeral_buffers"
)

var logCtx context.Context

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logCtx = log.Logger.WithContext(context.Background())

	status := m.Run()

	os.Exit(status)
}

func TestBasic(t *testing.T) {
	n := 10
	size := 1000

	pool := ephemeral_buffers.NewPool(logCtx, n, size)

	buf := pool.Acquire("TestBasic")
	buf.Write([]byte("Hello"))

	if string(buf.Bytes()) != "Hello" {
		t.Errorf("incorrect result")
	}

	if pool.BuffersAvailable() != n - 1 {
		t.Errorf("incorrect result")
	}

	buf.Release()

	if pool.BuffersAvailable() != n {
		t.Errorf("incorrect result")
	}

	pool.Free()
}

func TestAcquireRelease(t *testing.T) {
	n := 10
	size := 1000

	pool := ephemeral_buffers.NewPool(logCtx, n, size)

	b0 := pool.Acquire("TestAcquireRelease")

	go func() {
		time.Sleep(500 * time.Millisecond)
		b0.Release()
	}()

	buffers := []*ephemeral_buffers.Buffer{}

	for i := 0; i < n; i++ {
		s := fmt.Sprintf("test string %d", i + 1)

		b := pool.Acquire("TestAcquireRelease")
		b.Write([]byte(s))

		buffers = append(buffers, b)

		t.Logf("Allocated buffer with content: %s", s)
	}

	for _, b := range buffers {
		t.Logf("Releasing buffer with content: %s", b.String())
		b.Release()
	}

	pool.Free()
}

func TestOverflow(t *testing.T) {
	pool := ephemeral_buffers.NewPool(logCtx, 1, 1024)

	t.Logf("Expect warnings:")

	for i := 4; i < 10000; i++ {
		b0 := pool.Acquire("TestOverflow")

		for j := 0; j < i; j++ {
			b0.Write([]byte{1, 2, 3, 4})
		}

		b0.Release()
	}
}
