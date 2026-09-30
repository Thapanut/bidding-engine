-- Atomically decides one bid. Mirrors domain.Auction.Place; the two are held
-- to the same behaviour by the shared decision test suite.
--
-- KEYS[1] auction hash
-- KEYS[2] accepted-bids hash (bid_id -> record), the idempotency ledger
-- KEYS[3] recent-bids list ("bid_id|record", newest first, capped) for live views
-- ARGV[1] bid_id         ARGV[2] bidder_id        ARGV[3] amount (satang)
-- ARGV[4] anti-sniping window ms   ARGV[5] extension ms   ARGV[6] retention ms
-- ARGV[7] recent bids kept
--
-- Reply: { code } or { code, payload }
--   ACCEPTED / DUPLICATE -> payload is the bid record
--       seq|bidder_id|amount|placed_at_ms|ends_at_ms|extended
--   TOO_LOW              -> payload is the minimum acceptable amount
local auction_key, bids_key, recent_key = KEYS[1], KEYS[2], KEYS[3]
local bid_id, bidder_id, amount_arg = ARGV[1], ARGV[2], ARGV[3]
local window_ms, extension_ms, retention_ms = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])

local replay = redis.call('HGET', bids_key, bid_id)
if replay then
  return { 'DUPLICATE', replay }
end

local a = redis.call('HMGET', auction_key,
  'seller_id', 'start_price', 'min_increment', 'current_price',
  'highest_bidder', 'seq', 'starts_at_ms', 'ends_at_ms')
if not a[1] then
  return { 'NOT_FOUND' }
end

-- Redis is the single clock for every decision, so app-server skew cannot
-- reorder bids or reopen an ended auction.
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local starts_ms, ends_ms = tonumber(a[7]), tonumber(a[8])
if now_ms < starts_ms or now_ms >= ends_ms then
  return { 'NOT_ACTIVE' }
end

if bidder_id == a[1] then
  return { 'SELF_BID' }
end
if bidder_id == a[5] then
  return { 'ALREADY_HIGHEST' }
end

local amount = tonumber(amount_arg)
local min_required = tonumber(a[2])
if tonumber(a[6]) > 0 then
  min_required = tonumber(a[4]) + tonumber(a[3])
end
if amount < min_required then
  return { 'TOO_LOW', string.format('%.0f', min_required) }
end

local extended = 0
if ends_ms - now_ms <= window_ms then
  ends_ms = now_ms + extension_ms
  extended = 1
end

local seq = redis.call('HINCRBY', auction_key, 'seq', 1)
local ends_str = string.format('%.0f', ends_ms)
redis.call('HSET', auction_key,
  'current_price', amount_arg,
  'highest_bidder', bidder_id,
  'ends_at_ms', ends_str)

local record = table.concat({
  string.format('%.0f', seq), bidder_id, amount_arg,
  string.format('%.0f', now_ms), ends_str, tostring(extended),
}, '|')
redis.call('HSET', bids_key, bid_id, record)
redis.call('LPUSH', recent_key, bid_id .. '|' .. record)
redis.call('LTRIM', recent_key, 0, tonumber(ARGV[7]) - 1)

local expire_at = string.format('%.0f', ends_ms + retention_ms)
redis.call('PEXPIREAT', auction_key, expire_at)
redis.call('PEXPIREAT', bids_key, expire_at)
redis.call('PEXPIREAT', recent_key, expire_at)

return { 'ACCEPTED', record }
