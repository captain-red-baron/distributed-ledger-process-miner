import pandas as pd
import numpy as np


def compute_transitions(event_log: pd.DataFrame) -> (pd.DataFrame, pd.Series):
    """
    Takes an event log and computes the transition matrix and its aggregation. The transition matrix shows in a
    ordered fashion the transition from on transaction type to another coded as a category string value.
    Example: 'UtC->CtU'
    :param event_log: a pandas dataframe with the columns 'total_pos', 'action.input', 'blockNumber',
    'senderIsContract', 'senderIsERC20', 'sender_id', 'receiverIsContract', 'receiverIsERC20',
   'receiver_id', 'timestamp', 'transaction_id', 'sender_type', 'receiver_type', 'transaction_type'
    :param save_intermediate: save intermediate values as csv. True by default
    :param save_prefix: prefix for the saved file names.
    :param save_name: the name of the file to save.
    :returns pd.DataFrame: the transitions, pd.Series: the transition occurrence frequencies as a Series.
    """
    event_log['transaction_type'] = event_log['transaction_type'].astype('category')
    event_log['receiver_type'] = event_log['transaction_type'].astype('category')
    event_log['sender_type'] = event_log['transaction_type'].astype('category')

    # Perform matrix shifts in two directions
    event_log['prev_transaction_type'] = event_log['transaction_type'].shift(1)
    event_log['prev_transaction_id'] = event_log['transaction_id'].shift(1)
    event_log['next_transaction_type'] = event_log['transaction_type'].shift(-1)
    event_log['next_transaction_id'] = event_log['transaction_id'].shift(-1)
    event_log['curr_transition'] = event_log.apply(
        lambda x: get_curr_trans(x['prev_transaction_id'], x['prev_transaction_type'], x['transaction_id'],
                                 x['transaction_type']), axis=1)
    event_log['next_transition'] = event_log.apply(
        lambda x: get_next_trans(x['transaction_id'], x['transaction_type'], x['next_transaction_id']), axis=1)

    # Perform "cog" movement
    event_log.index = event_log.index*2
    empty_frame = pd.DataFrame(np.nan, index=range(0, len(event_log)), columns=event_log.columns)
    empty_frame.index = empty_frame.index * 2 - 1
    empty_frame = empty_frame.drop(empty_frame.index[0])
    full_frame = pd.concat([event_log, empty_frame])
    full_frame = full_frame.sort_index(kind='mergesort')
    full_frame['next_shifted'] = full_frame['next_transition'].shift(1)
    full_frame = full_frame.dropna(how='all')
    full_frame['transition'] = full_frame.apply(lambda x: merge_transition(x['curr_transition'], x['next_shifted']),
                                                axis=1)
    full_frame = full_frame.fillna(method='ffill')
    full_frame['total_pos'] = full_frame['total_pos'].astype(np.int64)
    full_frame['timestamp'] = full_frame['timestamp'].astype(np.int64)
    full_frame = full_frame[['total_pos', 'timestamp', 'transition']]

    transition_agg = full_frame.groupby('transition')['transition'].count()
    transition_agg = transition_agg.fillna(0)
    transition_agg = transition_agg.astype(np.int64)

    return full_frame, transition_agg


def compute_dependency_confidence(aggregated_transitions: pd.Series) -> pd.Series:
    """
    Computes the confidence of relationships between to transaction types:
    0 -> no relation ship,
    +-1 -> strong relationship
    :param aggregated_transitions: a pd.Series, according to the outcome of 'compute_transitions'
    :returns confidence_series: a pd.Series illustration how strong the dependency in process terms from two transaction
    types towards each other is.
    """
    confidence_series = pd.Series()
    for key in aggregated_transitions.keys():
        act_a, act_b = key.split('->')
        try:
            awb = aggregated_transitions['{}->{}'.format(act_a, act_b)]
        except KeyError as e:
            awb = 0

        try:
            bwa = aggregated_transitions['{}->{}'.format(act_b, act_a)]
        except KeyError as e:
            bwa = 0

        if awb == bwa:
            confidence_series[key] = 1
        else:
            confidence_series[key] = (awb - bwa) / (awb + bwa)
    return confidence_series


def get_curr_trans(prev_id, prev_type, curr_id, curr_type) -> str:
    """
    Evaluate the current transition based on the information from the current and the previous transaction
    :param prev_id: id of the previous transaction
    :param prev_type: type of the previous transaction
    :param curr_id: id of the current transaction
    :param curr_type: type of the current transaction
    :returns sta->E string, representing a transition from the start event to an event E or E->E' if not a start event
    """
    if float(prev_id) != float(curr_id):
        return 'sta->{}'.format(curr_type)
    else:
        return '{}->{}'.format(prev_type, curr_type)


def get_next_trans(curr_id, curr_type, next_id):
    """
    Evaluate the next transition based on the information from the current and the next transaction
    :param curr_id:  id of the current transaction
    :param curr_type: type of the current transaction
    :param next_id:  id of the next transaction
    :return: E->end as string if the current id and the next id differ (a case found its end), np.nan otherwise
    """
    if float(curr_id) != float(next_id):
        return '{}->end'.format(curr_type)
    else:
        return np.nan


def merge_transition(trans, nxt):
    """
    Gets the transiton for the merge operations of two columns
    :param trans:
    :param nxt:
    :return:
    """
    if type(trans) == str:
        return trans
    else:
        return nxt


def compute_dependency_graph(transitions: pd.Series, dep: pd.Series, relative_cutoff: float, significance_cutoff: float) -> nx.DiGraph:
    total_transitions = transitions.sum()
    transitions_relative = transitions / total_transitions

    g = nx.DiGraph()
    node_set = set()
    for i, v in transitions.items():
        current_transition = i
        act_a = current_transition[:3]
        act_b = current_transition[-3:]
        node_set.add(act_a)
        node_set.add(act_b)
    for act in node_set:
        g.add_node(act)
    for i, v in transitions.items():
        current_transition = i
        act_a = current_transition[:3]
        act_b = current_transition[-3:]
        if transitions_relative[current_transition] > relative_cutoff and dep[current_transition] > significance_cutoff:
            g.add_edge(act_a, act_b)
            g[act_a][act_b]['relative'] = round(transitions_relative[current_transition], 4)
            g[act_a][act_b]['significance'] = round(dep[current_transition], 2)
            g[act_a][act_b]['label'] = '{}% ({})'.format(round(transitions_relative[current_transition] * 100, 2), round(dep[current_transition], 2))
    return g