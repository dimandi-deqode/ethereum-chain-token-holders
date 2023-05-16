from web3 import Web3
from db import connect_db
from erc20_abi import ERC20_ABI
from psycopg2 import DatabaseError
import psycopg2
import time

# Create a new connection object
conn = connect_db()

# Create a cursor object to execute SQL statements on the database
cur = conn.cursor()

def setup_database():
    """
    Set up the database by executing the create queries from the schema file.
    """
    try:
        with open('db_schema.sql', 'r') as file:
            create_sqls = file.read()
        cur.execute(create_sqls)
        conn.commit()
    except DatabaseError as e:
        print("An error occurred while initializing the database:", str(e))
        # Re-raise the exception
        raise

# Set the Infura API key
infura_api_key = 'f8139559fa204800936ef846e3c5ccc6'

# Initialize web3 instance
w3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_api_key}'))

# Set the tokens whose holders are to be fetched
token_addresses = {
    '0xdAC17F958D2ee523a2206206994597C13D831ec7': 'USDT',
    '0xB8c77482e45F1F44dE1745F52C74426C631bDD52': 'BNB',
    '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984': 'UNI',
    '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9': 'AAVE',
    '0x514910771AF9Ca656af840dff83E8264EcF986CA': 'LINK'
}

# Dictionary that maps token contract addresses to token contract instance objects
token_contracts = {}

def build_token_contracts():
    """
    Build the token contract instances for the mentioned tokens.
    """
    try:
        for token_contract_address in token_addresses:
            # Create a contract instance
            token_contract = w3.eth.contract(
                address=Web3.toChecksumAddress(token_contract_address),
                abi=ERC20_ABI
            )
            token_contracts[token_contract_address] = token_contract
    except Exception as e:
        print("An error occurred while building the token contacts:", str(e))
        # Re-raise the exception
        raise

def get_token_holders(block_number):
    """
    Get token holders from a block and store them in the database.
    """
    max_retries = 3  # Maximum retries
    cur_retry = 1  # Current retry
    
    while cur_retry <= max_retries:
        try:
            # Get all transactions in the block
            block = w3.eth.get_block(block_number, full_transactions=True)
            transactions = block['transactions']

            # Loop through all transactions in the block
            for transaction in transactions:
                try:
                    transaction_receipt = w3.eth.get_transaction_receipt(transaction.hash)
                    # Skip the transaction if it is not successful
                    if not transaction_receipt or transaction_receipt.status == 0:
                        continue
                    to_address = transaction_receipt['to']
                    from_address = transaction_receipt['from']
                    if to_address in token_addresses:
                        token_symbol = token_addresses[to_address]
                        value = transaction['value']
                        transaction_hash = transaction_receipt['transactionHash']

                        # Insert the token transaction to the database
                        cur.execute("""
                            INSERT INTO token_transactions (transaction_hash, token_symbol, from_address, to_address, value) 
                            VALUES (%(hash)s, %(symbol)s, %(from_address)s, %(to_address)s, %(value)s) 
                        """, {'hash': transaction_hash, 'symbol': token_symbol, 'from_address': from_address, 'to_address': to_address, 'value': value})

                        holder_address = from_address
                        balance = token_contracts[to_address].functions.balanceOf(holder_address).call()

                        # Insert the token holder to the database
                        cur.execute("""
                            INSERT INTO token_holders (token_symbol, holder_address, balance)
                            VALUES (%(symbol)s, %(address)s, %(balance)s)
                            ON CONFLICT (token_symbol, holder_address) DO UPDATE
                            SET balance = %(balance)s
                        """, {'symbol': token_symbol, 'address': holder_address, 'balance': balance})
                except Exception as e:
                    # Rollback changes and re-raise the exception
                    conn.rollback()
                    raise e

            # Commit changes to the database
            conn.commit()
            
            # Exit the loop if all transactions are processed successfully
            break

        except Exception as e:
            # Handle exception 
            print(f"Error retrieving block/transaction information: {e}")

            # Retry the operation if there are remaining attempts
            if cur_retry < max_retries:
                cur_retry += 1
                print(f"Retrying the operation (Attempt {cur_retry})...")
                time.sleep(5)  # Wait for a short period before retrying
                continue
            else:
                print("Exceeded maximum retry attempts. Exiting...")
                # Re-raise the exception
                raise

def update_block_number_table(block_number):
    """
    Update the ethereum_block_number table with the provided block number.
    """
    try:
        cur.execute("SELECT COUNT(*) FROM ethereum_block_number")
        row_count = cur.fetchone()[0]

        # Insert the block number if the table is empty, else update the existing
        if row_count == 0:
            cur.execute("INSERT INTO ethereum_block_number (block_number) VALUES (%s)", (block_number,))
        else:
            cur.execute("UPDATE ethereum_block_number SET block_number = %s", (block_number,))

        # Commit changes to the database
        conn.commit()

    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while updating the block number table:", str(e))
        # Re-raise the exception
        raise

def fetch_prev_block_number():
    """
    Get the previous block number from the database.
    """
    try:
        cur.execute("SELECT COUNT(*) FROM ethereum_block_number")
        row_count = cur.fetchone()[0]

        # Fetch the previous block number from the table if it's not empty,
        # else set it to one less than the current block in the blockchain
        if row_count == 0:
            prev_block_number = w3.eth.blockNumber - 1
        else:
            cur.execute("SELECT block_number FROM ethereum_block_number")
            prev_block_number = cur.fetchone()[0]

        return prev_block_number
    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while fetching the previous block number from the database:", str(e))
        raise
    except Exception as e:
        # Handle the exception
        print("An error occurred while setting the previous block number:", str(e))
        # Re-raise the exception
        raise

if __name__ == '__main__':
    
    try:
        # Set up the database
        setup_database()

        # Build the token_contracts dictionary
        build_token_contracts()

        # Get the previous block number
        prev_block_number = fetch_prev_block_number()

        # Main loop
        while True:
            try:
                # Get the current block number
                cur_block_number = w3.eth.blockNumber

                # Get the token holders from the recently added blocks
                if cur_block_number != prev_block_number:
                    for block_number in range(prev_block_number + 1, cur_block_number + 1):
                        get_token_holders(block_number)
                        update_block_number_table(block_number)
            except Exception as e:
                break
            
            # Update the previous block number
            prev_block_number = cur_block_number
    except Exception as e:
        pass