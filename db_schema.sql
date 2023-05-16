-- Creates a new table to store the token holders
CREATE TABLE IF NOT EXISTS token_holders (
    token_symbol VARCHAR(50),
    holder_address VARCHAR(255),
    balance NUMERIC(30, 2),
    PRIMARY KEY (token_symbol, holder_address)
);

-- Creates a new table to store the token transactions
CREATE TABLE IF NOT EXISTS token_transactions (
    transaction_hash VARCHAR(255) PRIMARY KEY,
    token_symbol VARCHAR(50),
    from_address VARCHAR(255),
    to_address VARCHAR(255),
    value NUMERIC(30, 2)
);

-- Creates a new table to store the last fetched block number
CREATE TABLE IF NOT EXISTS ethereum_block_number (
    block_number BIGINT PRIMARY KEY
);