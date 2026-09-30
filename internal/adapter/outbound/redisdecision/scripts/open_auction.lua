-- Opens an auction for bidding. Idempotent: an auction that is already open
-- is left untouched so live state is never reset.
--
-- KEYS[1] auction hash
-- ARGV[1] seller_id      ARGV[2] start_price   ARGV[3] min_increment
-- ARGV[4] starts_at_ms   ARGV[5] ends_at_ms    ARGV[6] expire_at_ms
-- ARGV[7] title
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'seller_id', ARGV[1],
  'title', ARGV[7],
  'start_price', ARGV[2],
  'min_increment', ARGV[3],
  'current_price', ARGV[2],
  'highest_bidder', '',
  'seq', '0',
  'starts_at_ms', ARGV[4],
  'ends_at_ms', ARGV[5])
redis.call('PEXPIREAT', KEYS[1], ARGV[6])
return 1
