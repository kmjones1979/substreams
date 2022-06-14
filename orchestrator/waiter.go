package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type Waiter interface {
	Wait(ctx context.Context) <-chan interface{}
	Signal(storeName string, blockNum uint64)
	Size() int
	String() string
}

type waiterItem struct {
	StoreName string
	BlockNum  uint64 // This job requires waiting on this particular block number to be unblocked.

	closeOnce sync.Once
	waitChan  chan interface{}
}

func (wi *waiterItem) Close() {
	wi.closeOnce.Do(func() {
		close(wi.waitChan)
	})
}

func (wi *waiterItem) Wait() <-chan interface{} {
	return wi.waitChan
}

func (wi *waiterItem) String() string {
	return fmt.Sprintf("waiter (store:%s) (block:%d)", wi.StoreName, wi.BlockNum)
}

type waiterItems []*waiterItem

func (wis waiterItems) String() string {
	var wislice []string
	for _, wi := range wis {
		wislice = append(wislice, wi.String())
	}

	return strings.Join(wislice, ",")
}

type BlockWaiter struct {
	items []*waiterItem

	setup sync.Once
	done  chan interface{}
}

func NewWaiter(blockNum uint64, stores ...*pbsubstreams.Module) *BlockWaiter {
	var items []*waiterItem

	for _, store := range stores {
		if blockNum <= store.InitialBlock {
			continue
		}

		items = append(items, &waiterItem{
			StoreName: store.Name,
			BlockNum:  blockNum,
			waitChan:  make(chan interface{}),
		})
	}

	return &BlockWaiter{
		items: items,
	}
}

func (w *BlockWaiter) Wait(ctx context.Context) <-chan interface{} {
	w.setup.Do(func() {
		w.done = make(chan interface{})
		if len(w.items) == 0 {
			close(w.done)
			return
		}

		wg := sync.WaitGroup{}
		wg.Add(len(w.items))

		go func() {
			wg.Wait()
			close(w.done)
		}()

		for _, waiter := range w.items {
			go func(waiter *waiterItem) {
				defer wg.Done()

				select {
				case <-waiter.Wait():
					return
				case <-ctx.Done():
					return
				}

			}(waiter)
		}
	})

	return w.done
}

func (w *BlockWaiter) Signal(storeName string, blockNum uint64) {
	for _, waiter := range w.items {
		if waiter.StoreName != storeName {
			continue
		}

		if waiter.BlockNum > blockNum {
			continue
		}

		waiter.Close()
	}
}

func (w *BlockWaiter) Size() int {
	return len(w.items)
}

func (w *BlockWaiter) String() string {
	if w.items == nil {
		return fmt.Sprintf("[%s] O(%d)", "nil", w.Size())
	}

	var wis []string
	for _, wi := range w.items {
		wis = append(wis, wi.String())
	}

	return fmt.Sprintf("[%s] O(%d)", strings.Join(wis, ","), w.Size())
}
