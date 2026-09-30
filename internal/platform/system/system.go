// Package system provides the production clock and ID generator.
package system

import (
	"time"

	"github.com/google/uuid"
)

// Clock reads the wall clock.
type Clock struct{}

// Now returns the current time.
func (Clock) Now() time.Time { return time.Now() }

// UUIDGenerator produces random UUIDs.
type UUIDGenerator struct{}

// NewID returns a new UUIDv4 string.
func (UUIDGenerator) NewID() string { return uuid.NewString() }
