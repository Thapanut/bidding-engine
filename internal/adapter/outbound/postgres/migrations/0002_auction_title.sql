-- Optional display title for the auction catalogue.
ALTER TABLE auctions ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT '';
