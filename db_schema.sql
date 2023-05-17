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
CREATE TABLE IF NOT EXISTS constants (
    block_number BIGINT
);

-- Creates a new table to store the hashes of the transactions that are left out to be processed
CREATE TABLE IF NOT EXISTS batch_state (
    block_number BIGINT,
    transaction_hash VARCHAR(255) PRIMARY KEY
);

-- Creates a new table to store the block numbers in the current batch that have been processed
CREATE TABLE IF NOT EXISTS processed_blocks (
    block_number BIGINT
);