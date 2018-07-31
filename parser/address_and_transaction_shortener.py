from sys import argv
import glob
import datetime
import pandas as pd
import time

from config.constants import RAW_TRANSACTION_COLUMN_NAMES, \
    CONTRACT_LOOKUP_COLUMN_NAMES, \
    BLOCK_TIMES_COLUMN_NAMES

import logging
logging.basicConfig(filename='address_shortner.log', level=logging.DEBUG)


def check_data_fame_conformance(df, pattern):
    if pattern.issubset(df.columns):
        return True
    else:
        return False


script, arg0, arg1, arg2, arg3 = argv

pd.options.mode.chained_assignment = None

path_to_raw_transaction_bulk = arg0
#input("Please paste the path to raw transaction bulk directory (e.g. /Users/mister-x/[...]/parity_data)")
    #'/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/parity_data'
path_contracts_lookup = arg1
#input("Please paste the path to the contracts lookup csv file (e.g. /Users/mister-x/[...]/parity_data/contractsWithERCFlags.csv)")
    #'/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/contractsWithERCFlags.csv'

address_lookup_filename = arg2
#input("Please paste Path and name to the addresses lookup output csv file (e.g. /Users/mister-x/[...]/parity_data/address_lookup.csv)")

transaction_lookup_filename = arg3
#input("Please paste Path and name to the transactions lookup output csv file (e.g. /Users/mister-x/[...]/parity_data/transaction_lookup.csv)")

print("{}- Applying now shortening. Please wait...".format(datetime.datetime.now()))
logging.info("{}- Applying now shortening. Please wait...".format(datetime.datetime.now()))
contracts_lookup = pd.read_csv(path_contracts_lookup)

logging.info('Checking data')
if not check_data_fame_conformance(contracts_lookup, CONTRACT_LOOKUP_COLUMN_NAMES):
    raise SyntaxError(
        'The column names of the contracts lookup csv file do no match the required column names: {}'.format(CONTRACT_LOOKUP_COLUMN_NAMES))

address_set = set()
transaction_hashes_set = set()

for filename in glob.iglob('{}/**/*.csv'.format(path_to_raw_transaction_bulk), recursive=True):
    raw_transactions = pd.read_csv(filename)
    if not check_data_fame_conformance(raw_transactions, RAW_TRANSACTION_COLUMN_NAMES):
        logging.info('{} - File {} not matching the raw transaction pattern. Skipping now'.format(datetime.datetime.now(), filename))
        print('{} - File {} not matching the raw transaction pattern. Skipping now'.format(datetime.datetime.now(), filename))
    else:
        logging.info('{} - File {}  matching the raw transaction pattern. Applying shortening'.format(datetime.datetime.now(), filename))
        print('{} - File {}  matching the raw transaction pattern. Applying shortening'.format(datetime.datetime.now(), filename))
        from_addresses = raw_transactions['action.from'].unique()
        from_addresses = set(address for address in from_addresses)
        to_addresses = raw_transactions['action.to'].unique()
        to_addresses = set(address for address in to_addresses)

        logging.info('{} - Adding addresses from {} to set'.format(datetime.datetime.now(), filename))
        print('{} - Adding addresses from {} to set'.format(datetime.datetime.now(), filename))
        address_set = address_set.union(from_addresses).union(to_addresses)

        logging.info('{} - Adding transactions from {} to set'.format(datetime.datetime.now(), filename))
        print('{} - Adding transactions from {} to set'.format(datetime.datetime.now(), filename))
        transactions = raw_transactions['transactionHash'].unique()
        transactions = set(transaction for transaction in transactions)
        transaction_hashes_set = transaction_hashes_set.union(transactions)

addresses_df = pd.DataFrame(list(address_set))
addresses_df.columns = ['address_hex']
addresses_df = addresses_df.dropna()

# Get the contract lookup and set is Contract flag
contracts_lookup['isContract'] = True
contracts_lookup['address_hex'] = contracts_lookup['result.address']
contracts_lookup = contracts_lookup[['address_hex', 'isContract', 'isERC20']]
addresses_lookup = pd.merge(addresses_df, contracts_lookup, left_on='address_hex',
                            right_on='address_hex', how='outer')
addresses_lookup['id'] = addresses_lookup.index
addresses_lookup.set_index('address_hex')

transaction_hashes = pd.DataFrame(list(transaction_hashes_set))
transaction_hashes['id'] = transaction_hashes.index
transaction_hashes.columns = ['transaction_hash', 'id']
transaction_hashes.set_index('transaction_hash')

logging.info('{} - Saving now global lookup tables to {} and {}'.format(datetime.datetime.now(), address_lookup_filename, transaction_lookup_filename))
print('{} - Saving now global lookup tables to {} and {}'.format(datetime.datetime.now(), address_lookup_filename, transaction_lookup_filename))
addresses_lookup.to_csv(address_lookup_filename, index=False)
transaction_hashes.to_csv(transaction_lookup_filename, index=False)
logging.info('{} - Saved global lookup tables'.format(datetime.datetime.now()))
print('{} - Saved global lookup tables'.format(datetime.datetime.now()))




