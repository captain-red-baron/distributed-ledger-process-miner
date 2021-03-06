import pandas as pd
import numpy as np


def parse_event_log(raw_transactions: pd.DataFrame, transaction_lookup: pd.DataFrame, addresses_lookup: pd.DataFrame, block_times: pd.DataFrame,
                    block_padding=1000000000)->(pd.DataFrame, pd.DataFrame):
    """
    Parses an event log from a raw transactions dataFrame.
    :param raw_transactions: the raw transactions to parse from. Should include at least the following columns:
    'id', 'action.from', 'action.input', 'action.to', 'blockNumber', 'transactionHash', 'type'
    :param transaction_lookup: a lookup table with all transaction hashes mapped to integers. Columns:
    'transaction_hash', 'id'.
    :param addresses_lookup: a lookup table with all addresses and information if the address is a
    contract. Columns: 'address_hex', 'isContract', 'isERC20', 'id'
    :param block_times: a lookup table to map the block numbers to their times for time series analysis.
    :param block_padding: a kwarg to show the padding of the block. The single transaction segments in the outcome
    transaction log will have an unique id, ascending in time. The outcome is call 'total_pos' and is computed as
    current_block_number * block_padding + line number. Hence the block_padding kwarg should be selected as
    min(10^x: 10^x >len(raw_transactions)). Per default 100M.
    :return: events, trace complexities (by days)
    """
    addresses_lookup.set_index('address_hex')
    transaction_lookup.set_index('transaction_hash')

    # Events
    raw_transactions['id'] = raw_transactions['blockNumber'] * block_padding + raw_transactions.index
    raw_transactions = raw_transactions[['id', 'action.from', 'action.input', 'action.to', 'blockNumber', 'transactionHash', 'type']]
    raw_transactions.set_index('id')

    events = raw_transactions.merge(addresses_lookup, left_on='action.from', right_on='address_hex', how='left') \
        .merge(addresses_lookup, left_on='action.to', right_on='address_hex', how='left') \
        .merge(block_times, left_on='blockNumber', right_on='number', how='inner') \
        .merge(transaction_lookup, left_on='transactionHash', right_on='transaction_hash', how='inner')

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

    # traces by day
    traces_by_day = pd.DataFrame()
    events['day'] = pd.to_datetime(events['timestamp'], unit='s').dt.normalize()
    groups = events.groupby('day')
    for day, group in groups:
        trace_lengths = group.groupby('transaction_id').count().groupby('total_pos').count()['action.input']
        trace_lengths['day'] = day
        traces_by_day = traces_by_day.append(trace_lengths, ignore_index=True)
    traces_by_day = traces_by_day.set_index('day')
    traces_by_day = traces_by_day.fillna(0)
    traces_by_day = traces_by_day.astype(np.int64)
    events = events.drop('day', axis=1)

    return events, traces_by_day


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
