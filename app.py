from web3 import Web3
from db import connect_db
from erc20_abi import ERC20_ABI
import psycopg2

# Create a new connection object
conn = connect_db()

# Create a cursor object to execute SQL statements on the database
cur = conn.cursor()

# Read the database schema file
with open('init.sql', 'r') as file:
    init_sqls = file.read()

# Execute the queries from the schema file to create tables
cur.execute(init_sqls)

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

# Dictionary that maps token contract addresses to token contract instance objects
TOKEN_CONTRACTS = {}

# Function to build the token contract instances for the mentioned tokens
def build_token_contracts():
    for token_contract_address in TOKEN_ADDRESSES:
        # Create a contract instance
        token_contract = w3.eth.contract(address=Web3.toChecksumAddress(token_contract_address), abi=ERC20_ABI)
        TOKEN_CONTRACTS[token_contract_address] = token_contract
        
# Function to get token holders from a block
def get_token_holders(block_number):
    # Get all transactions in the block
    block = w3.eth.get_block(block_number, full_transactions=True)
    transactions = block['transactions']

    # Loop through all transactions in the block
    for transaction in transactions:
        transaction_receipt = w3.eth.get_transaction_receipt(transaction.hash)
        # Check if the transaction is successful
        if transaction_receipt and transaction_receipt.status == 1:
            to_address = transaction_receipt['to']
            from_address = transaction_receipt['from']
            if to_address in TOKEN_ADDRESSES:
                token_symbol = TOKEN_ADDRESSES[to_address]
                value = transaction['value']
                # Add the token transaction to the database
                cur.execute("INSERT INTO token_transactions (token_symbol, from_address, to_address, value) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (token_symbol, from_address, to_address, value))
                
                holder_address = from_address
                balance = TOKEN_CONTRACTS[to_address].functions.balanceOf(holder_address).call()
                # Add the holder to the database
                cur.execute("INSERT INTO token_holders (token_symbol, holder_address, balance) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (token_symbol, holder_address, balance))
    
    # Commit changes to the database
    conn.commit()

if __name__ == '__main__':
    # Build the TOKEN_CONTRACTS dictionary
    build_token_contracts()

    # Get the previous block number
    prev_block_number = w3.eth.blockNumber - 1

    # Main loop
    while True:
        # Get the current block number
        cur_block_number = w3.eth.blockNumber

        # Update the block number in the database
        cur.execute("UPDATE ethereum_block_number SET block_number = %s", (cur_block_number,))
        conn.commit()

        # Get the token holders from the recently added blocks
        if cur_block_number != prev_block_number:
            for block_number in range(prev_block_number + 1, cur_block_number + 1):
                get_token_holders(block_number)
        
        # Update the previous block number
        prev_block_number = cur_block_number