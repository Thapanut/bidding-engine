package httpapi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Thapanut/bidding-engine/internal/adapter/inbound/httpapi"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/memory"
	"github.com/Thapanut/bidding-engine/internal/core/auction/service"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
)

type api struct {
	app       *httpapi.Server
	publisher *memory.Publisher
}

func newAPI() api {
	clock, ids := system.Clock{}, system.UUIDGenerator{}
	publisher := memory.NewPublisher()
	bidding := service.NewBidding(memory.NewDecisionStore(clock), publisher, memory.NewRepository(), clock, ids)
	return api{
		app:       httpapi.New(bidding, ids, clock, slog.New(slog.NewTextHandler(io.Discard, nil))),
		publisher: publisher,
	}
}

func (a api) do(t *testing.T, method, path, body string, headers map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := a.app.Test(req, -1)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("%s %s: response is not JSON: %s", method, path, raw)
		}
	}
	return resp, decoded
}

func (a api) createAuction(t *testing.T, id string) {
	t.Helper()
	now := time.Now().UTC()
	body, _ := json.Marshal(map[string]any{
		"auction_id": id, "seller_id": "seller-1",
		"start_price_satang": 10_000, "min_increment_satang": 500,
		"starts_at": now.Add(-time.Hour), "ends_at": now.Add(time.Hour),
	})
	if resp, decoded := a.do(t, http.MethodPost, "/v1/auctions", string(body), nil); resp.StatusCode != http.StatusCreated {
		t.Fatalf("create auction: %d %v", resp.StatusCode, decoded)
	}
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func TestAC14_BidAndStatusHappyPath(t *testing.T) {
	a := newAPI()
	a.createAuction(t, "auction-1")

	resp, bid := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body = %v", resp.StatusCode, bid)
	}
	if bid["amount_satang"] != float64(10_000) || bid["sequence"] != float64(1) || bid["extended"] != false {
		t.Fatalf("bid = %v", bid)
	}
	if key := resp.Header.Get(httpapi.HeaderIdempotencyKey); key == "" || key != bid["bid_id"] {
		t.Fatalf("Idempotency-Key header = %q, bid_id = %v", key, bid["bid_id"])
	}

	resp, status := a.do(t, http.MethodGet, "/v1/auctions/auction-1", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %v", resp.StatusCode, status)
	}
	want := map[string]any{
		"status": "active", "current_price_satang": float64(10_000), "min_next_bid_satang": float64(10_500),
		"highest_bidder_id": "bidder-1", "bid_count": float64(1),
	}
	for field, value := range want {
		if status[field] != value {
			t.Errorf("%s = %v, want %v", field, status[field], value)
		}
	}
}

func TestAC14_IdempotencyKeyReplaysTheSameBid(t *testing.T) {
	a := newAPI()
	a.createAuction(t, "auction-1")
	headers := map[string]string{httpapi.HeaderIdempotencyKey: "key-1"}
	const body = `{"bidder_id":"bidder-1","amount_satang":10000}`

	_, first := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", body, headers)
	resp, second := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", body, headers)
	if resp.StatusCode != http.StatusCreated || second["bid_id"] != "key-1" || second["sequence"] != first["sequence"] || second["placed_at"] != first["placed_at"] {
		t.Fatalf("replay = %d %v, want the original %v", resp.StatusCode, second, first)
	}

	resp, reused := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":99999}`, headers)
	if resp.StatusCode != http.StatusUnprocessableEntity || errorCode(reused) != "idempotency_key_reused" {
		t.Fatalf("key reuse = %d %v", resp.StatusCode, reused)
	}
}

func TestAC15_ErrorContract(t *testing.T) {
	a := newAPI()
	a.createAuction(t, "auction-1")
	if resp, body := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`, nil); resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed bid: %d %v", resp.StatusCode, body)
	}

	cases := []struct {
		name, method, path, body string
		status                   int
		code                     string
	}{
		{"unknown auction", http.MethodPost, "/v1/auctions/missing/bids", `{"bidder_id":"b","amount_satang":1}`, http.StatusNotFound, "auction_not_found"},
		{"unknown auction status", http.MethodGet, "/v1/auctions/missing", "", http.StatusNotFound, "auction_not_found"},
		{"seller bids on own auction", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"seller-1","amount_satang":20000}`, http.StatusForbidden, "self_bidding"},
		{"highest bidder bids again", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":20000}`, http.StatusConflict, "already_highest_bidder"},
		{"below min increment", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10499}`, http.StatusUnprocessableEntity, "bid_too_low"},
		{"fractional money", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10500.5}`, http.StatusBadRequest, "validation_failed"},
		{"unknown field", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10500,"x":1}`, http.StatusBadRequest, "validation_failed"},
		{"trailing data", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10500}{}`, http.StatusBadRequest, "validation_failed"},
		{"zero amount", http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":0}`, http.StatusBadRequest, "validation_failed"},
		{"missing bidder", http.MethodPost, "/v1/auctions/auction-1/bids", `{"amount_satang":10500}`, http.StatusBadRequest, "validation_failed"},
		{"invalid auction", http.MethodPost, "/v1/auctions", `{"seller_id":"s","start_price_satang":0}`, http.StatusBadRequest, "validation_failed"},
		{"unknown route", http.MethodGet, "/v1/nope", "", http.StatusNotFound, "request_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := a.do(t, tc.method, tc.path, tc.body, nil)
			if resp.StatusCode != tc.status || errorCode(body) != tc.code {
				t.Fatalf("got %d %v, want %d %s", resp.StatusCode, body, tc.status, tc.code)
			}
		})
	}

	_, body := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10499}`, nil)
	if e := body["error"].(map[string]any); e["min_required_satang"] != float64(10_500) {
		t.Fatalf("bid_too_low body = %v, want min_required_satang 10500", body)
	}
}

func TestAC15_RejectsBidOnEndedAuction(t *testing.T) {
	a := newAPI()
	now := time.Now().UTC()
	body, _ := json.Marshal(map[string]any{
		"auction_id": "ended", "seller_id": "seller-1", "start_price_satang": 10_000, "min_increment_satang": 500,
		"starts_at": now.Add(-2 * time.Hour), "ends_at": now.Add(-time.Hour),
	})
	a.do(t, http.MethodPost, "/v1/auctions", string(body), nil)

	resp, decoded := a.do(t, http.MethodPost, "/v1/auctions/ended/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`, nil)
	if resp.StatusCode != http.StatusConflict || errorCode(decoded) != "auction_not_active" {
		t.Fatalf("got %d %v", resp.StatusCode, decoded)
	}
}

func TestAC12_PublishFailureReturns503WithRetryableKey(t *testing.T) {
	a := newAPI()
	a.createAuction(t, "auction-1")
	a.publisher.FailWith(errors.New("broker down: secret-host:9092"))

	resp, body := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`, nil)
	if resp.StatusCode != http.StatusServiceUnavailable || errorCode(body) != "bid_not_durable" {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
	key := resp.Header.Get(httpapi.HeaderIdempotencyKey)
	if key == "" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("503 must carry Idempotency-Key and Retry-After, got %v", resp.Header)
	}
	if raw, _ := json.Marshal(body); bytes.Contains(raw, []byte("secret-host")) {
		t.Fatalf("response leaks internals: %s", raw)
	}

	a.publisher.FailWith(nil)
	resp, bid := a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`,
		map[string]string{httpapi.HeaderIdempotencyKey: key})
	if resp.StatusCode != http.StatusCreated || bid["sequence"] != float64(1) || len(a.publisher.Published()) != 1 {
		t.Fatalf("retry = %d %v, published = %d", resp.StatusCode, bid, len(a.publisher.Published()))
	}
}

func TestHealthz(t *testing.T) {
	resp, _ := newAPI().do(t, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestAC24_ListAuctionsAndRecentBids(t *testing.T) {
	a := newAPI()
	now := time.Now().UTC()
	body, _ := json.Marshal(map[string]any{
		"auction_id": "auction-1", "title": "Toyota Hilux Revo", "seller_id": "seller-1",
		"start_price_satang": 10_000, "min_increment_satang": 500,
		"starts_at": now.Add(-time.Hour), "ends_at": now.Add(time.Hour),
	})
	if resp, decoded := a.do(t, http.MethodPost, "/v1/auctions", string(body), nil); resp.StatusCode != http.StatusCreated || decoded["title"] != "Toyota Hilux Revo" {
		t.Fatalf("create: %d %v", resp.StatusCode, decoded)
	}
	a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-1","amount_satang":10000}`, nil)
	a.do(t, http.MethodPost, "/v1/auctions/auction-1/bids", `{"bidder_id":"bidder-2","amount_satang":10500}`, nil)

	resp, list := a.do(t, http.MethodGet, "/v1/auctions", "", nil)
	auctions, _ := list["auctions"].([]any)
	if resp.StatusCode != http.StatusOK || len(auctions) != 1 || list["server_time"] == nil {
		t.Fatalf("list: %d %v", resp.StatusCode, list)
	}
	if lot := auctions[0].(map[string]any); lot["title"] != "Toyota Hilux Revo" || lot["bid_count"] != float64(2) || lot["current_price_satang"] != float64(10_500) {
		t.Fatalf("listed auction = %v", lot)
	}

	resp, history := a.do(t, http.MethodGet, "/v1/auctions/auction-1/bids", "", nil)
	bids, _ := history["bids"].([]any)
	if resp.StatusCode != http.StatusOK || len(bids) != 2 {
		t.Fatalf("bids: %d %v", resp.StatusCode, history)
	}
	if newest := bids[0].(map[string]any); newest["bidder_id"] != "bidder-2" || newest["sequence"] != float64(2) {
		t.Fatalf("newest bid = %v, want bidder-2 at sequence 2", newest)
	}

	resp, empty := a.do(t, http.MethodGet, "/v1/auctions/unknown/bids", "", nil)
	if bids, _ := empty["bids"].([]any); resp.StatusCode != http.StatusOK || len(bids) != 0 || empty["bids"] == nil {
		t.Fatalf("unknown auction bids: %d %v, want an empty array", resp.StatusCode, empty)
	}
}

func TestAC25_ServesTheArenaPage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := newAPI().app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	page, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("status = %d, content type = %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	for _, want := range []string{"<!doctype html>", "EventSource", "/v1/auctions"} {
		if !bytes.Contains(page, []byte(want)) {
			t.Errorf("page does not contain %q", want)
		}
	}
}

type liveEvent struct {
	Auction struct {
		CurrentPriceSatang int64  `json:"current_price_satang"`
		HighestBidderID    string `json:"highest_bidder_id"`
		BidCount           int64  `json:"bid_count"`
	} `json:"auction"`
	RecentBids []struct {
		BidderID string `json:"bidder_id"`
	} `json:"recent_bids"`
	ServerTime time.Time `json:"server_time"`
}

// readEvent returns the next server-sent event satisfying accept.
func readEvent(t *testing.T, events *bufio.Reader, accept func(liveEvent) bool) liveEvent {
	t.Helper()
	for {
		line, err := events.ReadString('\n')
		if err != nil {
			t.Fatalf("read event stream: %v", err)
		}
		payload, isData := strings.CutPrefix(line, "data: ")
		if !isData {
			continue
		}
		var event liveEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			t.Fatalf("event is not JSON: %q", payload)
		}
		if accept(event) {
			return event
		}
	}
}

func TestAC26_EventStreamPushesBidsAndStopsOnShutdown(t *testing.T) {
	a := newAPI()
	a.createAuction(t, "auction-1")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = a.app.Listener(listener)
	}()
	base := "http://" + listener.Addr().String()

	client := &http.Client{Timeout: 30 * time.Second}
	if resp, err := client.Get(base + "/v1/auctions/missing/events"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown auction stream: %v %v, want 404", resp, err)
	} else {
		resp.Body.Close()
	}

	resp, err := client.Get(base + "/v1/auctions/auction-1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	events := bufio.NewReader(resp.Body)

	first := readEvent(t, events, func(liveEvent) bool { return true })
	if first.Auction.BidCount != 0 || first.ServerTime.IsZero() {
		t.Fatalf("first event = %+v, want the opening state with a server time", first)
	}

	bid, err := client.Post(base+"/v1/auctions/auction-1/bids", "application/json",
		strings.NewReader(`{"bidder_id":"bidder-1","amount_satang":10000}`))
	if err != nil || bid.StatusCode != http.StatusCreated {
		t.Fatalf("place bid: %v %v", bid, err)
	}
	bid.Body.Close()

	pushed := readEvent(t, events, func(e liveEvent) bool { return e.Auction.BidCount == 1 })
	if pushed.Auction.CurrentPriceSatang != 10_000 || pushed.Auction.HighestBidderID != "bidder-1" ||
		len(pushed.RecentBids) != 1 || pushed.RecentBids[0].BidderID != "bidder-1" {
		t.Fatalf("pushed event = %+v, want the accepted bid", pushed)
	}

	// With nothing changing, the heartbeat still arrives.
	readEvent(t, events, func(e liveEvent) bool { return e.ServerTime.After(pushed.ServerTime) })

	// Shutdown must end open streams instead of waiting on them.
	if err := a.app.ShutdownWithTimeout(10 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	<-served
	if _, err := io.Copy(io.Discard, events); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Logf("stream ended with: %v", err)
	}
}
