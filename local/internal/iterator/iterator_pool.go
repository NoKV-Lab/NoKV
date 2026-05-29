// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator

import (
	"sync"
	"sync/atomic"

	index "github.com/feichai0017/NoKV/txn/storage"
)

// IteratorContext owns the temporary iterator slice used to assemble one DB
// iterator instance.
type IteratorContext struct {
	iters []index.Iterator
}

func (ctx *IteratorContext) Append(iters ...index.Iterator) {
	if ctx == nil {
		return
	}
	ctx.iters = append(ctx.iters, iters...)
}

func (ctx *IteratorContext) Iterators() []index.Iterator {
	if ctx == nil {
		return nil
	}
	return ctx.iters
}

func (ctx *IteratorContext) reset() {
	if ctx == nil {
		return
	}
	ctx.iters = ctx.iters[:0]
}

// IteratorPool reuses per-iterator temporary contexts on the DB read path.
type IteratorPool struct {
	pool  sync.Pool
	reuse atomic.Uint64
}

func NewIteratorPool() *IteratorPool {
	ip := &IteratorPool{}
	ip.pool.New = func() any { return nil }
	return ip
}

func (p *IteratorPool) Get() *IteratorContext {
	if p == nil {
		return &IteratorContext{iters: make([]index.Iterator, 0, 8)}
	}
	if v := p.pool.Get(); v != nil {
		if ctx, ok := v.(*IteratorContext); ok {
			p.reuse.Add(1)
			ctx.reset()
			return ctx
		}
	}
	return &IteratorContext{iters: make([]index.Iterator, 0, 8)}
}

func (p *IteratorPool) Put(ctx *IteratorContext) {
	if p == nil || ctx == nil {
		return
	}
	ctx.reset()
	p.pool.Put(ctx)
}

func (p *IteratorPool) Reused() uint64 {
	if p == nil {
		return 0
	}
	return p.reuse.Load()
}
