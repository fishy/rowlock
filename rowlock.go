package rowlock

import (
	"sync"

	"go.yhsif.com/defaultdict"
)

// NewLocker defines a type of function that can be used to create a new Locker.
type NewLocker func() sync.Locker

// RWLocker is the abstracted interface of sync.RWMutex.
type RWLocker interface {
	sync.Locker

	RLocker() sync.Locker
}

// Make sure that sync.RWMutex is compatible with RWLocker interface.
var _ RWLocker = (*sync.RWMutex)(nil)

// MutexNewLocker is a NewLocker using sync.Mutex.
func MutexNewLocker() sync.Locker {
	return new(sync.Mutex)
}

// RWMutexNewLocker is a NewLocker using sync.RWMutex.
func RWMutexNewLocker() sync.Locker {
	return new(sync.RWMutex)
}

// RowLock defines a set of locks.
//
// When you do Lock/Unlock operations, you don't do them on a global scale.
// Instead, a Lock/Unlock operation is operated on a given row.
//
// If NewLocker returns an implementation of RWLocker in NewRowLock,
// the RowLock can be locked separately for read in RLock and RUnlock functions.
// Otherwise, RLock is the same as Lock and RUnlock is the same as Unlock.
type RowLock[T comparable] struct {
	d defaultdict.Map[T, sync.Locker]
}

// NewRowLock creates a new RowLock with the given NewLocker.
func NewRowLock[T comparable](f NewLocker) *RowLock[T] {
	return &RowLock[T]{
		d: defaultdict.New[T](defaultdict.Generator[sync.Locker](f)),
	}
}

// Lock locks a row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
func (rl *RowLock[T]) Lock(row T) {
	rl.getLocker(row).Lock()
}

// Unlock unlocks a row.
func (rl *RowLock[T]) Unlock(row T) {
	rl.getLocker(row).Unlock()
}

// RLock locks a row for read.
//
// It only works as expected when NewLocker specified in NewRowLock returns an
// implementation of RWLocker. Otherwise, it's the same as Lock.
func (rl *RowLock[T]) RLock(row T) {
	rl.getRLocker(row).Lock()
}

// RUnlock unlocks a row for read.
//
// It only works as expected when NewLocker specified in NewRowLock returns an
// implementation of RWLocker. Otherwise, it's the same as Unlock.
func (rl *RowLock[T]) RUnlock(row T) {
	rl.getRLocker(row).Unlock()
}

// getLocker returns the lock for the given row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
func (rl *RowLock[T]) getLocker(row T) sync.Locker {
	return rl.d.Get(row)
}

// getRLocker returns the lock for read for the given row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
//
// If NewLocker specified in NewRowLock returns a locker that didn't implement
// GetRLocker, the locker itself will be returned instead.
func (rl *RowLock[T]) getRLocker(row T) sync.Locker {
	locker := rl.getLocker(row)
	if rwlocker, ok := locker.(RWLocker); ok {
		return rwlocker.RLocker()
	}
	return locker
}
