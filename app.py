from web3 import Web3
from db import connect_db
from psycopg2 import DatabaseError
from multiprocessing import Pool
import psycopg2
import time

def setup_database():
    """
    Set up the database by executing the create queries from the schema file.
    """
    try:
        # Create a new connection object and cursor object
        conn = connect_db()
        cur = conn.cursor()
        with open('db_schema.sql', 'r') as file:
            create_sqls = file.read()
        cur.execute(create_sqls)
        conn.commit()
    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while initializing the database: ")
        conn.rollback()
        # Re-raise the exception
        raise
    finally:
        # Close the database connection
        cur.close()
        conn.close()

# Set the Infura API key
infura_api_key = 'd74e509efb9648ac9123cc7e51283cac'

# Set the transfer event signature
transfer_event_signature = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

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

def insert_transaction_hashes(conn, cur, block_number, transaction_hashes):
    """
    Insert the transaction hashes with its block number to save the state
    """
    try:
        # Iterate over the transactions and insert them into the state table
        for transaction_hash in transaction_hashes:
            cur.execute("INSERT INTO batch_state (block_number, transaction_hash) VALUES (%s, %s)", (block_number, transaction_hash))

        # Commit the changes to the database
        conn.commit()
    except DatabaseError as e:
        print("An error occured while inserting the transaction hashes!")
        # Re-raise the exception
        raise

def delete_transaction(conn, cur, transaction_hash):
    """
    Delete the processed transaction hash.
    """
    try:
        # Delete the transaction from the state table
        cur.execute("DELETE FROM batch_state WHERE transaction_hash = %s", (transaction_hash,))

        # Commit the changes to the database
        conn.commit()
    except DatabaseError as e:
        print("An error occured while deleting the transaction hash!")
        # Re-raise the exception
        raise

def process_transaction(conn, cur, transaction_hash):
    """
    Process the given transaction.
    """
    # Get the transaction reciept of the transaction and get the logs from it
    while True:
        try:
            # Extract the transaction receipt from web3
            transaction_receipt = w3.eth.get_transaction_receipt(transaction_hash)
            logs_list = transaction_receipt['logs']
            break
        except Exception as e:
            # Handle the exception
            print("Retrying for the transaction: ", transaction_hash)
            time.sleep(1)
            continue

    try:
        # Skip the transaction if it is not successful
        if not transaction_receipt or transaction_receipt['status'] == 0 or len(logs_list) == 0:
            delete_transaction(conn, cur, transaction_hash)
            return
        logs_data = logs_list[0]
        
        # Skip the transaction if it is not transfer event
        if 'topics' not in logs_data or len(logs_data['topics']) != 3 or logs_data['topics'][0].hex() != transfer_event_signature:
            delete_transaction(conn, cur, transaction_hash)
            return

        # Get the token contract address of the transferred token
        to_address_token = transaction_receipt['to']

        # Skip the transaction if the transferred token is not in the considered tokens list
        if to_address_token not in token_addresses:
            delete_transaction(conn, cur, transaction_hash)
            return

        # Extract the from and to addresses
        from_address = Web3.toChecksumAddress(logs_data['topics'][1].hex()[26:])
        to_address = Web3.toChecksumAddress(logs_data['topics'][2].hex()[26:])
        
        # Extract the value
        value = int(logs_data['data'], 16)

        # Get the token symbol
        token_symbol = token_addresses[to_address_token]
    
        # Insert the token transaction to the database
        cur.execute("""
            INSERT INTO token_transactions (transaction_hash, token_symbol, from_address, to_address, value) 
            VALUES (%(hash)s, %(symbol)s, %(from_address)s, %(to_address)s, %(value)s) 
        """, {'hash': transaction_hash, 'symbol': token_symbol, 'from_address': from_address, 'to_address': to_address, 'value': value})
        
        # Upsert the balances for both from and to addresses
        upsert_query = """
            INSERT INTO token_holders (token_symbol, holder_address, balance)
            VALUES (%(symbol)s, %(address)s, %(balance)s)
            ON CONFLICT (token_symbol, holder_address)
            DO UPDATE SET balance = token_holders.balance + %(balance)s
        """
        cur.execute(upsert_query, {'symbol': token_symbol, 'address': from_address, 'balance': -value})
        cur.execute(upsert_query, {'symbol': token_symbol, 'address': to_address, 'balance': value})  

        # Delete the transaction hash from the state table
        cur.execute("DELETE FROM batch_state WHERE transaction_hash = %s", (transaction_hash,))  
        
        # Commit changes to the database
        conn.commit()
    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while processing the transaction!")
        # Re-raise the exception
        raise
    except Exception as e:
        # Handle the exception
        print("An error occurred while processing the transaction!")
        # Re-raise the exception
        raise
        
def process_block(block_number):
    """
    Process the given block.
    """
    try:
        # Create a new connection object and cursor object
        conn = connect_db()
        cur = conn.cursor()

        # Check if the current block is already processed
        cur.execute("SELECT COUNT(*) FROM processed_blocks WHERE block_number = %s", (block_number,))
        row_count = cur.fetchone()[0]
        if row_count != 0:
            return

        # Fetch the transaction hashes for the current block from the maintained state of the batch
        cur.execute("SELECT transaction_hash FROM batch_state WHERE block_number = %s", (block_number,))
        rows = cur.fetchall()

        # If there are no trasaction hashes for this block, extract them freshly from web3 and insert them into the state table
        # Else use the fetched transaction hashes left out in the table
        if cur.rowcount == 0:
            while True:
                try:
                    block = w3.eth.get_block(block_number)
                    transaction_hashes = block['transactions']
                    transaction_hashes = [transaction_hash.hex() for transaction_hash in transaction_hashes]
                    break
                except Exception as e:
                    # Handle the exception
                    print("Retrying for the block: ", block_number)
                    time.sleep(1)
                    continue
            insert_transaction_hashes(conn, cur, block_number, transaction_hashes)
        else:
            # Extract the transaction hashes from the rows got from the state table
            transaction_hashes = [row[0] for row in rows]
        
        # Process each transaction
        for transaction_hash in transaction_hashes:
            process_transaction(conn, cur, transaction_hash)
        
        # Insert the current block into the processed blocks
        cur.execute("INSERT INTO processed_blocks (block_number) VALUES (%s)", (block_number,))
        conn.commit()
    except DatabaseError as e:
        # Handle the exception
        print("An error occured while processing the block: ", block_number)
        conn.rollback()
        raise
    except Exception as e:
        # Handle the exception
        print("An error occured while processing the block: ", block_number)
        # print(e)
        raise
    finally:
        # Close the database connection
        cur.close()
        conn.close()

def update_block_number_table(block_number):
    """
    Update the constants table with the provided block number 
    and deletes all the records from the processed blocks table after the completed batch processing
    """
    try:
        # Create a new connection object and cursor object
        conn = connect_db()
        cur = conn.cursor()

        # Insert the block number if the table is empty, else update the existing
        cur.execute("SELECT COUNT(*) FROM constants")
        row_count = cur.fetchone()[0]
        if row_count == 0:
            cur.execute("INSERT INTO constants (block_number) VALUES (%s)", (block_number,))
        else:
            cur.execute("UPDATE constants SET block_number = %s", (block_number,))
        
        # Empty the processed blocks table
        cur.execute("DELETE FROM processed_blocks")

        # Commit changes to the database
        conn.commit()
    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while updating the block number table!")
        conn.rollback()
        # Re-raise the exception
        raise
    finally:
        # Close the database connection
        cur.close()
        conn.close()

def fetch_prev_block_number():
    """
    Get the previous block number from the database.
    """
    try:
        # Create a new connection object and cursor object
        conn = connect_db()
        cur = conn.cursor()

        # Fetch the previous block number from the table if it's not empty,
        # else set it to one less than the current block in the blockchain and add it to the table
        cur.execute("SELECT block_number FROM constants")
        if cur.rowcount == 0:
            while True:
                try:
                    prev_block_number = w3.eth.blockNumber - 1
                except Exception as e:
                    # Handle the exception
                    print("Retrying to get the previous block number...")
                    time.sleep(1)
                    continue
                # Add this previous block number to the constants table
                cur.execute("INSERT INTO constants (block_number) VALUES (%s)", (prev_block_number,))
                conn.commit()
                break
        else:
            prev_block_number = cur.fetchone()[0]

        return prev_block_number
    except DatabaseError as e:
        # Handle the exception
        print("An error occurred while fetching the previous block number from the database!")
        conn.rollback()
        # Re-raise the exception
        raise
    except Exception as e:
        # Handle the exception
        print("An error occurred while setting the previous block number!")
        # Re-raise the exception
        raise
    finally:
        # Close the database connection
        cur.close()
        conn.close()

def process_blocks_batch(start_block, end_block):
    """
    Multiprocessing function to process all the blocks in the given batch parallelly.
    """
    try:
        # Set the batch list and map them into the pool
        block_numbers = range(start_block, end_block + 1)
        with Pool() as pool:
            pool.map(process_block, block_numbers)
    except Exception as e:
        # Handle the exception
        print("An error occured while processing the blocks batch: ", start_block, end_block)
        # Re-raise the exception
        raise

if __name__ == '__main__':
    try:
        # Set up the database
        setup_database()

        # Get the previous block number
        prev_block_number = fetch_prev_block_number()
    except Exception as e:
        # Handle the exception
        print(e)
    else:
        # Main loop
        while True:
            # Get the latest block number
            try:
                cur_block_number = w3.eth.blockNumber
                # break
            except Exception as e:
                # Handle the exception
                print("Retrying to get the previous block number...")
                time.sleep(1)
                continue

            print("Latest block: ", cur_block_number)

            # Check if there are no new blocks
            if cur_block_number == prev_block_number:
                print("Waiting for new blocks to add...")
                time.sleep(3)
                continue

            # Set the range of blocks batch
            start_block = prev_block_number + 1
            end_block = min(cur_block_number, start_block + 10)

            try:
                # Start multiprocessing the batch
                print("processing batch: ", start_block, end_block)
                process_blocks_batch(start_block, end_block)
                print("processed batch: ", start_block, end_block)

                # Store the latest processed batch's end block number
                update_block_number_table(end_block)
            except Exception as e:
                # Handle the exception
                print(e)
                break

            # Update the previous block number
            prev_block_number = end_block
            print("=============================================================")