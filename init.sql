-- Creates a new table to store the token holders
CREATE TABLE IF NOT EXISTS token_holders (
    token_symbol VARCHAR(50),
    holder_address VARCHAR(255),
    balance NUMERIC(30, 2)
);

-- Creates a new table to store the token transactions
CREATE TABLE IF NOT EXISTS token_transactions (
    id serial PRIMARY KEY,
    token_symbol VARCHAR(50),
    from_address VARCHAR(255),
    to_address VARCHAR(255),
    value NUMERIC(30, 2)
);

-- Creates a new table to store the last fetched block number
CREATE TABLE IF NOT EXISTS ethereum_block_number (
    id SERIAL PRIMARY KEY,
    block_number BIGINT
);

-- Initiates a record
INSERT INTO ethereum_block_number (id)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM ethereum_block_number);
