-- Add up migration script here
CREATE TABLE IF NOT EXISTS trades (
  id BIGSERIAL,
  symbol VARCHAR(20) NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (id, time)
);

SELECT create_hypertable('trades', 'time');