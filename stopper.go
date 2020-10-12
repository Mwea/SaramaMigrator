package SaramaMigrator

import (
	"sync"
	"time"
)

// A Stopper signals completion over C by closing it when Stop is called
type Stopper struct {
	C    chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// NewStopper creates a new Stopper
func NewStopper() *Stopper {
	s := &Stopper{
		C: make(chan struct{}),
	}
	return s
}

// Add adds `delta` to the number of processes using the stopper
func (s *Stopper) Add(delta int) {
	s.wg.Add(delta)
}

// Done signals the stopper that one job waiting for this is complete.
func (s *Stopper) Done() {
	s.wg.Done()
}

// Stop closes C. It is safe to call multiple times.
func (s *Stopper) Stop() {
	s.once.Do(func() {
		close(s.C)
	})
}

// Stopped returns true if Stop has been called at least once. It doesn't mean
// all jobs waiting on this have finished.
func (s *Stopper) Stopped() bool {
	select {
	case <-s.C:
		return true
	default:
		return false
	}
}

// StopAndWait stops the stopper and waits for stopping to complete.
func (s *Stopper) StopAndWait() {
	s.Stop()
	s.wg.Wait()
}

// Every will run f on the given interval until the stopper is stopped
// (incrementing and decrementing the counter as well).  It polls in
// its own goroutine.
func (s *Stopper) Every(interval time.Duration, f func()) {
	if interval <= 0 {
		// time.NewTicker panics with non-positive intervals.
		// panic before launching the goroutine so we can see full stack trace.
		panic("non-positive interval for Stopper.Every")
	}
	s.Add(1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer s.Done()
		for {
			select {
			case <-ticker.C:
				f()
			case _, ok := <-s.C:
				if !ok {
					return
				}
			}
		}
	}()
}
