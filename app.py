from web3 import Web3
from db import connect_db
import psycopg2

# Create a new connection object
conn = connect_db()

# Create a cursor object to execute SQL statements on the database
cur = conn.cursor()

# Execute a CREATE TABLE statement to create a new table to store the token holders
cur.execute("""
    CREATE TABLE token_holders (
        token_symbol VARCHAR(50),
        holder_address VARCHAR(255)
    )
""")

# Commit the changes to the database
conn.commit()

# Set the infura API key
INFURA_API_KEY = 'f8139559fa204800936ef846e3c5ccc6'

# Initialize web3 instance
w3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{INFURA_API_KEY}'))

# Set the tokens whose holders are to be fetched
TOKEN_ADDRESSES = {
    '0xdAC17F958D2ee523a2206206994597C13D831ec7': 'USDT',
    '0xB8c77482e45F1F44dE1745F52C74426C631bDD52': 'BNB',
    '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984': 'UNI',
    '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9': 'AAVE',
    '0x514910771AF9Ca656af840dff83E8264EcF986CA': 'LINK'
}

# Function to get token holders from a block
def get_token_holders(block_number):
    # Get all transactions in the block
    block = w3.eth.getBlock(block_number, full_transactions=True)
    transactions = block['transactions']

    # Loop through all transactions in the block
    for transaction in transactions:
        transaction_receipt = w3.eth.getTransactionReceipt(transaction.hash)
        if transaction_receipt and transaction_receipt.status == 1:
            # Check if the transaction is successful
            to_address = transaction_receipt['to']
            from_address = transaction_receipt['from']
            if to_address in TOKEN_ADDRESSES:
                token_symbol = TOKEN_ADDRESSES[to_address]
                # Add the holder to the database
                holder_address = from_address
                cur.execute("INSERT INTO token_holders (token_symbol, holder_address) VALUES (%s, %s) ON CONFLICT DO NOTHING", (token_symbol, holder_address))
    
    # Commit changes to the database
    conn.commit()

if __name__ == '__main__':
    # Get the previous block number
    prev_block_number = w3.eth.blockNumber - 1

    # Main loop
    while True:
        # Get the current block number
        cur_block_number = w3.eth.blockNumber

        # Get the token holders from the recently added blocks
        if cur_block_number != prev_block_number:
            for block_number in range(prev_block_number + 1, cur_block_number + 1):
                get_token_holders(block_number)
        
        # Update the previous block number
        prev_block_number = cur_block_number


