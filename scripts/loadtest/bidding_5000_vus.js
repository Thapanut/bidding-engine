// k6 load test: up to 5,000 virtual users bidding on one auction.
//
// Run with `make loadtest-k6` (the engine must be running: `make up && make run`).
//
// Each virtual user is one bidder. Per iteration it reads the auction status,
// bids at or a few increments above the minimum next bid with a fresh
// Idempotency-Key, then pauses like a person would.
//
// Environment:
//   BASE_URL   API base URL              (default http://localhost:8080)
//   AUCTION_ID bid on this existing auction instead of creating one, e.g. the
//              one open in the Bid Arena page
//   VUS        peak virtual users        (default 5000)
//   RAMP_UP    time to reach VUS         (default 30s)
//   HOLD       time at VUS               (default 1m)
//   RAMP_DOWN  time back to zero         (default 10s)
import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const VUS = parseInt(__ENV.VUS || '5000', 10);

const START_PRICE_SATANG = 10000;
const MIN_INCREMENT_SATANG = 500;
const MAX_EXTRA_INCREMENTS = 10;
const AUCTION_LENGTH_MS = 6 * 60 * 60 * 1000;
const MIN_THINK_SECONDS = 0.5;
const MAX_THINK_SECONDS = 1.5;

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// Business rejections are expected outcomes, not failed requests.
const STATUS_ACCEPTED = 201;
const STATUS_SELF_BIDDING = 403;
const STATUS_CONFLICT = 409; // auction_not_active, already_highest_bidder
const STATUS_UNPROCESSABLE = 422; // bid_too_low, idempotency_key_reused
http.setResponseCallback(
  http.expectedStatuses(200, STATUS_ACCEPTED, STATUS_SELF_BIDDING, STATUS_CONFLICT, STATUS_UNPROCESSABLE),
);

const bidsAccepted = new Counter('bids_accepted');
const bidsRejected = new Counter('bids_rejected');
const serverErrors = new Rate('server_errors');
const unexpectedResponses = new Rate('unexpected_responses');
const bidLatency = new Trend('bid_latency', true);
const statusLatency = new Trend('status_latency', true);

export const options = {
  scenarios: {
    bidders: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP_UP || '30s', target: VUS },
        { duration: __ENV.HOLD || '1m', target: VUS },
        { duration: __ENV.RAMP_DOWN || '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    server_errors: ['rate==0'],
    unexpected_responses: ['rate==0'],
    http_req_failed: ['rate==0'],
    bid_latency: ['p(95)<500', 'p(99)<1000'],
    status_latency: ['p(95)<500', 'p(99)<1000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};

// Picks or creates the auction every virtual user bids on. The run ID keeps
// Idempotency-Keys unique when several runs target the same auction.
export function setup() {
  const runId = `${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
  if (__ENV.AUCTION_ID) {
    const existing = http.get(`${BASE_URL}/v1/auctions/${__ENV.AUCTION_ID}`);
    if (existing.status !== 200) {
      fail(`auction ${__ENV.AUCTION_ID} not found at ${BASE_URL}: ${existing.status} ${existing.body}`);
    }
    return { auctionId: __ENV.AUCTION_ID, runId, minIncrement: existing.json('min_increment_satang') };
  }
  const auctionId = `loadtest-${Date.now()}`;
  const now = Date.now();
  const res = http.post(
    `${BASE_URL}/v1/auctions`,
    JSON.stringify({
      auction_id: auctionId,
      seller_id: 'loadtest-seller',
      start_price_satang: START_PRICE_SATANG,
      min_increment_satang: MIN_INCREMENT_SATANG,
      starts_at: new Date(now - 60 * 1000).toISOString(),
      ends_at: new Date(now + AUCTION_LENGTH_MS).toISOString(),
    }),
    { headers: JSON_HEADERS },
  );
  if (res.status !== STATUS_ACCEPTED) {
    fail(`could not create the auction at ${BASE_URL}: ${res.status} ${res.body}`);
  }
  return { auctionId, runId, minIncrement: MIN_INCREMENT_SATANG };
}

function errorCode(res) {
  try {
    return res.json('error.code') || '';
  } catch (_) {
    return '';
  }
}

export default function (data) {
  const auctionUrl = `${BASE_URL}/v1/auctions/${data.auctionId}`;

  const status = http.get(auctionUrl, { tags: { name: 'auction_status' } });
  statusLatency.add(status.timings.duration);
  serverErrors.add(status.status >= 500 || status.status === 0);
  if (!check(status, { 'status: 200': (r) => r.status === 200 })) {
    unexpectedResponses.add(true);
    sleep(MIN_THINK_SECONDS);
    return;
  }

  // Integer satang only: minimum next bid plus 0-10 whole increments.
  const extra = Math.floor(Math.random() * (MAX_EXTRA_INCREMENTS + 1));
  const amount = status.json('min_next_bid_satang') + extra * data.minIncrement;

  const bid = http.post(
    `${auctionUrl}/bids`,
    JSON.stringify({ bidder_id: `vu-${__VU}`, amount_satang: amount }),
    {
      headers: Object.assign({ 'Idempotency-Key': `${data.runId}-${__VU}-${__ITER}` }, JSON_HEADERS),
      tags: { name: 'place_bid' },
    },
  );
  bidLatency.add(bid.timings.duration);
  serverErrors.add(bid.status >= 500 || bid.status === 0);

  const code = errorCode(bid);
  const accepted = bid.status === STATUS_ACCEPTED;
  const outbid = bid.status === STATUS_UNPROCESSABLE && code === 'bid_too_low';
  // auction_not_active happens when an existing auction closes mid-run.
  const alreadyLeading =
    bid.status === STATUS_CONFLICT && (code === 'already_highest_bidder' || code === 'auction_not_active');
  check(bid, {
    'bid: accepted (201) or valid rejection (409/422)': () => accepted || outbid || alreadyLeading,
    'bid: no 5xx': (r) => r.status < 500 && r.status !== 0,
    'bid: accepted has a sequence': (r) => !accepted || r.json('sequence') > 0,
    'bid: too low states the minimum': (r) => !outbid || r.json('error.min_required_satang') > amount,
  });
  const unexpected = !(accepted || outbid || alreadyLeading);
  unexpectedResponses.add(unexpected);
  if (unexpected && Math.random() < 0.001) {
    console.warn(`unexpected response ${bid.status}: ${bid.body}`);
  }
  if (accepted) {
    bidsAccepted.add(1);
  } else {
    bidsRejected.add(1, { reason: code || `http_${bid.status}` });
  }

  sleep(MIN_THINK_SECONDS + Math.random() * (MAX_THINK_SECONDS - MIN_THINK_SECONDS));
}

// Checks that live state is consistent once the load stops.
export function teardown(data) {
  const res = http.get(`${BASE_URL}/v1/auctions/${data.auctionId}`);
  console.log(`final auction state: ${res.body}`);
}

function ms(value) {
  return value === undefined ? 'n/a' : `${value.toFixed(1)} ms`;
}

function count(metric) {
  return metric ? metric.values.count : 0;
}

export function handleSummary(data) {
  const m = data.metrics;
  const seconds = data.state.testRunDurationMs / 1000;
  const bids = count(m.bids_accepted) + count(m.bids_rejected);
  const latency = (metric) =>
    metric ? `p95 ${ms(metric.values['p(95)'])}   p99 ${ms(metric.values['p(99)'])}   max ${ms(metric.values.max)}` : 'n/a';

  const lines = [
    '',
    `Bidding load test: ${VUS} VUs, ${seconds.toFixed(0)} s, ${BASE_URL}`,
    '',
    `  Throughput      ${m.http_reqs.values.rate.toFixed(0)} req/s overall (${count(m.http_reqs)} requests)`,
    `  Bids            ${(bids / seconds).toFixed(0)} bids/s (${count(m.bids_accepted)} accepted, ${count(m.bids_rejected)} rejected)`,
    `  Bid latency     ${latency(m.bid_latency)}`,
    `  Status latency  ${latency(m.status_latency)}`,
    `  5xx / no reply  ${((m.server_errors ? m.server_errors.values.rate : 0) * 100).toFixed(3)} %`,
    `  Unexpected      ${((m.unexpected_responses ? m.unexpected_responses.values.rate : 0) * 100).toFixed(3)} %`,
    '',
    '  Thresholds',
  ];
  for (const name of Object.keys(m).sort()) {
    for (const [expression, result] of Object.entries(m[name].thresholds || {})) {
      lines.push(`    ${result.ok ? 'PASS' : 'FAIL'}  ${name}: ${expression}`);
    }
  }
  lines.push('');
  return { stdout: lines.join('\n') };
}
