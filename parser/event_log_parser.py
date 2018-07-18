import pandas as pd
import numpy as np


def parse_event_log(raw_transactions: pd.DataFrame, contracts_lookup: pd.DataFrame, block_times: pd.DataFrame,
                    block_padding=1000000000)->(pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Parses an event log from a raw transactions dataFrame.
    :param raw_transactions: the raw transactions to parse from. Should include at least the following columns:
    'id', 'action.from', 'action.input', 'action.to', 'blockNumber', 'transactionHash', 'type'
    :param contracts_lookup: a lookup table with all contract addresses saved. Has the following columns
    'blockNumber', 'result.address', 'isERC20', 'timestamp'.
    :param block_times: a lookup table to map the block numbers to their times for time series analysis.
    :param block_padding: a kwarg to show the padding of the block. The single transaction segments in the outcome
    transaction log will have an unique id, ascending in time. The outcome is call 'total_pos' and is computed as
    current_block_number * block_padding + line number. Hence the block_padding kwarg should be selected as
    min(10^x: 10^x >len(raw_transactions)). Per default 100M.
    :return: addresses_lookup, transaction_hashes, events
    """
    from_addresses = raw_transactions['action.from'].unique()
    from_addresses = set(address for address in from_addresses)
    to_addresses = raw_transactions['action.to'].unique()
    to_addresses = set(address for address in to_addresses)

    addresses = from_addresses.union(to_addresses)
    addresses_df = pd.DataFrame(list(addresses))
    addresses_df.columns = ['address_hex']
    addresses_df = addresses_df.dropna()

    # Get the contract lookup and set is Contract flag
    contracts_lookup['isContract'] = True
    contracts_lookup['address_hex'] = contracts_lookup['result.address'].apply(lambda x: hex(int(x)))
    contracts_lookup = contracts_lookup[['address_hex', 'isContract', 'isERC20']]
    addresses_lookup = pd.merge(addresses_df, contracts_lookup, left_on='address_hex',
                                right_on='address_hex', how='outer')
    addresses_lookup['id'] = addresses_lookup.index
    addresses_lookup.set_index('address_hex')

    transaction_hashes = pd.DataFrame(list(raw_transactions['transactionHash'].unique()))
    transaction_hashes['id'] = transaction_hashes.index
    transaction_hashes.columns = ['transaction_hash', 'id']
    transaction_hashes.set_index('transaction_hash')

    # Events
    raw_transactions['id'] = raw_transactions['blockNumber'] * block_padding + raw_transactions.index
    raw_transactions = raw_transactions[['id', 'action.from', 'action.input', 'action.to', 'blockNumber', 'transactionHash', 'type']]
    raw_transactions.set_index('id')

    events = raw_transactions.merge(addresses_lookup, left_on='action.from', right_on='address_hex', how='left') \
        .merge(addresses_lookup, left_on='action.to', right_on='address_hex', how='left') \
        .merge(block_times, left_on='blockNumber', right_on='number', how='inner') \
        .merge(transaction_hashes, left_on='transactionHash', right_on='transaction_hash', how='inner')

    events.columns = ['total_pos', 'action.from', 'action.input', 'action.to', 'blockNumber', 'transactionHash',
                      'type', 'sender_address', 'senderIsContract', 'senderIsERC20', 'sender_id',
                      'receiver_address', 'receiverIsContract', 'receiverIsERC20', 'receiver_id', 'number',
                      'timestamp', 'transaction_hash_z', 'transaction_id']
    events = events.drop('action.from', axis=1)
    events = events.drop('action.to', axis=1)
    events = events.drop('sender_address', axis=1)
    events = events.drop('receiver_address', axis=1)
    events = events.drop('number', axis=1)
    events = events.drop('transaction_hash_z', axis=1)
    events = events.drop('transactionHash', axis=1)
    events = events.drop('blockNumber', axis=1)

    events['senderIsContract'] = events['senderIsContract'].fillna(False)
    events['senderIsERC20'] = events['senderIsERC20'].fillna(False)
    events['receiverIsContract'] = events['receiverIsContract'].fillna(False)
    events['receiverIsERC20'] = events['receiverIsERC20'].fillna(False)

    events['sender_id'] = events['sender_id'].fillna(-1)
    events['sender_id'] = events['sender_id'].astype(np.int64)
    events['receiver_id'] = events['receiver_id'].fillna(-1)
    events['receiver_id'] = events['receiver_id'].astype(np.int64)

    # get rid of contract creation transactions
    events = events[events['type'] == 'call']
    events = events.drop('type', axis=1)
    events['sender_type'] = events['senderIsContract'].apply(lambda x: check_user_type(x))
    events['receiver_type'] = events['receiverIsContract'].apply(lambda x: check_user_type(x))
    events['transaction_type'] = events['sender_type'].map(str) + 't' + events['receiver_type'].map(str)

    events['transaction_type'] = events['transaction_type'].astype('category')
    events['receiver_type'] = events['receiver_type'].astype('category')
    events['sender_type'] = events['receiver_type'].astype('category')

    return addresses_lookup, transaction_hashes, events


def check_user_type(is_contract: bool) -> str:
    """
    Checks the type of a actor in the network
    :param is_contract: boolean
    :return: string C or U for contract or user.
    """
    if is_contract == True:
        return 'C'
    else:
        return 'U'
