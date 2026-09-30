-- Durable system of record for auctions and the bid audit trail.
-- All money columns are BIGINT satang.

CREATE TABLE IF NOT EXISTS auctions (
    id                   TEXT        PRIMARY KEY,
    seller_id            TEXT        NOT NULL,
    start_price_satang   BIGINT      NOT NULL CHECK (start_price_satang > 0),
    min_increment_satang BIGINT      NOT NULL CHECK (min_increment_satang > 0),
    current_price_satang BIGINT      NOT NULL CHECK (current_price_satang > 0),
    highest_bidder_id    TEXT        NOT NULL DEFAULT '',
    bid_sequence         BIGINT      NOT NULL DEFAULT 0 CHECK (bid_sequence >= 0),
    starts_at            TIMESTAMPTZ NOT NULL,
    ends_at              TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (ends_at > starts_at)
);

-- Append-only audit of accepted bids. The primary key deduplicates
-- redelivered events; the sequence constraint guards the decision order.
CREATE TABLE IF NOT EXISTS bids (
    auction_id      TEXT        NOT NULL REFERENCES auctions (id),
    id              TEXT        NOT NULL,
    bidder_id       TEXT        NOT NULL,
    amount_satang   BIGINT      NOT NULL CHECK (amount_satang > 0),
    sequence        BIGINT      NOT NULL CHECK (sequence > 0),
    placed_at       TIMESTAMPTZ NOT NULL,
    auction_ends_at TIMESTAMPTZ NOT NULL,
    extended        BOOLEAN     NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (auction_id, id),
    UNIQUE (auction_id, sequence)
);
