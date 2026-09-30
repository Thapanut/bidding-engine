package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Thapanut/bidding-engine/internal/core/auction/domain"
	"github.com/Thapanut/bidding-engine/internal/kernel"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newAuction(t *testing.T) domain.Auction {
	t.Helper()
	a, err := domain.NewAuction("auction-1", "seller-1", 10_000, 500, t0, t0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func attempt(t *testing.T, id, bidder string, amount kernel.Money) domain.BidAttempt {
	t.Helper()
	a, err := domain.NewBidAttempt(id, "auction-1", bidder, amount)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewAuction_Validation(t *testing.T) {
	end := t0.Add(time.Hour)
	cases := []struct {
		name             string
		id, seller       string
		start, increment kernel.Money
		startsAt, endsAt time.Time
	}{
		{"empty id", "", "s", 1, 1, t0, end},
		{"id with key-breaking characters", "a{b}|c", "s", 1, 1, t0, end},
		{"id too long", string(make([]byte, 65)), "s", 1, 1, t0, end},
		{"empty seller", "a", "", 1, 1, t0, end},
		{"zero start price", "a", "s", 0, 1, t0, end},
		{"start price above cap", "a", "s", kernel.MaxMoney + 1, 1, t0, end},
		{"zero increment", "a", "s", 1, 0, t0, end},
		{"ends before it starts", "a", "s", 1, 1, end, t0},
		{"missing times", "a", "s", 1, 1, time.Time{}, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := domain.NewAuction(tc.id, tc.seller, tc.start, tc.increment, tc.startsAt, tc.endsAt)
			if !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("err = %v, want ErrValidation", err)
			}
		})
	}
}

func TestNewBidAttempt_Validation(t *testing.T) {
	for _, amount := range []kernel.Money{0, -1, kernel.MaxMoney + 1} {
		if _, err := domain.NewBidAttempt("bid-1", "auction-1", "bidder-1", amount); !errors.Is(err, domain.ErrValidation) {
			t.Fatalf("amount %d: err = %v, want ErrValidation", int64(amount), err)
		}
	}
	if _, err := domain.NewBidAttempt("", "auction-1", "bidder-1", 1); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("empty key: err = %v, want ErrValidation", err)
	}
}

func TestStatusAt(t *testing.T) {
	a := newAuction(t)
	cases := map[time.Time]domain.Status{
		t0.Add(-time.Nanosecond):        domain.StatusScheduled,
		t0:                              domain.StatusActive,
		t0.Add(time.Hour - time.Second): domain.StatusActive,
		t0.Add(time.Hour):               domain.StatusEnded,
	}
	for at, want := range cases {
		if got := a.StatusAt(at); got != want {
			t.Errorf("StatusAt(%v) = %s, want %s", at, got, want)
		}
	}
}

func TestPlace_AntiSnipingBoundary(t *testing.T) {
	end := t0.Add(time.Hour)
	cases := []struct {
		name     string
		now      time.Time
		extended bool
		wantEnd  time.Time
	}{
		{"one millisecond outside the window", end.Add(-domain.AntiSnipingWindow - time.Millisecond), false, end},
		{"exactly at the window edge", end.Add(-domain.AntiSnipingWindow), true, end},
		{"last millisecond", end.Add(-time.Millisecond), true, end.Add(-time.Millisecond).Add(domain.AntiSnipingExtension)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated, bid, err := newAuction(t).Place(attempt(t, "bid-1", "bidder-1", 10_000), tc.now)
			if err != nil {
				t.Fatal(err)
			}
			if bid.Extended != tc.extended || !bid.AuctionEndsAt.Equal(tc.wantEnd) || !updated.EndsAt.Equal(tc.wantEnd) {
				t.Fatalf("extended=%v ends_at=%v, want extended=%v ends_at=%v", bid.Extended, bid.AuctionEndsAt, tc.extended, tc.wantEnd)
			}
		})
	}
}

func TestPlace_RepeatedSnipesKeepExtending(t *testing.T) {
	a := newAuction(t)
	now := a.EndsAt.Add(-time.Second)
	amount := kernel.Money(10_000)
	for i, bidder := range []string{"bidder-1", "bidder-2", "bidder-1"} {
		var (
			bid domain.Bid
			err error
		)
		a, bid, err = a.Place(attempt(t, "bid-"+bidder+string(rune('a'+i)), bidder, amount), now)
		if err != nil {
			t.Fatal(err)
		}
		if !bid.Extended || !a.EndsAt.Equal(now.Add(domain.AntiSnipingExtension)) {
			t.Fatalf("snipe %d did not extend: %+v", i, bid)
		}
		now = a.EndsAt.Add(-time.Second)
		amount += a.MinIncrement
	}
}

func TestPlace_RejectionLeavesAuctionUnchanged(t *testing.T) {
	a := newAuction(t)
	unchanged, _, err := a.Place(attempt(t, "bid-1", "seller-1", 10_000), t0)
	if !errors.Is(err, domain.ErrSelfBidding) || unchanged != a {
		t.Fatalf("err = %v, auction changed = %v", err, unchanged != a)
	}
}

func TestWithTitle(t *testing.T) {
	a := newAuction(t)
	titled, err := a.WithTitle("  Toyota Hilux Revo 2.4 ปี 2021  ")
	if err != nil || titled.Title != "Toyota Hilux Revo 2.4 ปี 2021" {
		t.Fatalf("title = %q, err = %v, want it trimmed", titled.Title, err)
	}
	if untitled, err := a.WithTitle(""); err != nil || untitled.Title != "" {
		t.Fatalf("empty title must be allowed, got %q, %v", untitled.Title, err)
	}
	for name, title := range map[string]string{
		"too long":          strings.Repeat("ก", 121),
		"control character": "line one\nline two",
	} {
		if _, err := a.WithTitle(title); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("%s: err = %v, want ErrValidation", name, err)
		}
	}
	if _, err := a.WithTitle(strings.Repeat("ก", 120)); err != nil {
		t.Fatalf("120 Thai characters must fit: %v", err)
	}
}
