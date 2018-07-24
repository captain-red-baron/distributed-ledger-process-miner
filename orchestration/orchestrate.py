import os
import ntpath
import glob
import time

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from parser import event_log_parser as parser
from miner import heuristic_miner as miner

import logging
logging.basicConfig(filename='process_miner_orchestration.log',level=logging.DEBUG)

pd.options.mode.chained_assignment = None

#Paths
path_to_raw_transaction_bulk = '/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/parity_data'
path_contracts_lookup = '/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/contractsWithERCFlags.csv'
path_block_times = '/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/blockTimes.csv'
output_path_prefix = 'out'
extension = 'csv'

# Constants
raw_transaction_columns = {'action.address', 'action.balance', 'action.callType', 'action.from',
       'action.gas', 'action.init', 'action.input', 'action.refundAddress',
       'action.to', 'action.value', 'blockHash', 'blockNumber', 'error',
       'result', 'result.address', 'result.code', 'result.gasUsed',
       'result.output', 'subtraces', 'traceAddress', 'transactionHash',
       'transactionPosition', 'type'}
contracts_lookup_columns = {'result.address', 'isERC20'}
block_times_columns = {'number', 'timestamp'}


def check_data_fame_conformance(df, pattern):
    if pattern.issubset(df.columns):
        return True
    else:
        return False


def mine_segment_by_day(parsed_events, name, suffix) -> (pd.DataFrame, pd.DataFrame):
    """
    Utility function to mine a DataFrame of parsed events aggregated by day.
    :param parsed_events: the input Data from the parser module.
    :param name: a unique name for the origin of the mined day (e.g. "trasactions5000000-5100000")
    :param suffix: a suffix to get added to any file name.
    :return: two data frames. One for the global dependencies, one for the confidences. Columns: 'day', 'CtC->CtC',
    'CtC->CtU', 'CtC->UtU', 'CtC->end', 'CtU->CtC', 'CtU->CtU','CtU->UtC', 'CtU->UtU', 'CtU->end', 'UtC->CtC',
    'UtC->CtU', 'UtC->UtC', 'UtC->UtU', 'UtC->end', 'UtU->CtC', 'UtU->CtU', 'UtU->UtC', 'UtU->UtU', 'UtU->end',
    'sta->CtC', 'sta->UtC', 'sta->UtU'
    """
    global_dependencies_l = pd.DataFrame(columns=['day', 'CtC->CtC', 'CtC->CtU', 'CtC->UtU', 'CtC->end', 'CtU->CtC', 'CtU->CtU',
       'CtU->UtC', 'CtU->UtU', 'CtU->end', 'UtC->CtC', 'UtC->CtU', 'UtC->UtC',
       'UtC->UtU', 'UtC->end', 'UtU->CtC', 'UtU->CtU', 'UtU->UtC', 'UtU->UtU',
       'UtU->end', 'sta->CtC', 'sta->UtC', 'sta->UtU'])
    global_confidences_l = pd.DataFrame(columns=['day', 'CtC->CtC', 'CtC->CtU', 'CtC->UtU', 'CtC->end', 'CtU->CtC', 'CtU->CtU',
       'CtU->UtC', 'CtU->UtU', 'CtU->end', 'UtC->CtC', 'UtC->CtU', 'UtC->UtC',
       'UtC->UtU', 'UtC->end', 'UtU->CtC', 'UtU->CtU', 'UtU->UtC', 'UtU->UtU',
       'UtU->end', 'sta->CtC', 'sta->UtC', 'sta->UtU'])
    parsed_events['day'] = pd.to_datetime(parsed_events['timestamp'], unit='s').dt.normalize()
    groups = parsed_events.groupby('day')
    for day, group in groups:
        start = time.time()
        transitions, transitions_agg = miner.compute_transitions(group)
        confidence = miner.compute_dependency_confidence(transitions_agg)
        transitions_agg['day'] = day
        global_dependencies_l = global_dependencies_l.append(transitions_agg, ignore_index=True)
        global_confidences_l = global_confidences_l.append(confidence, ignore_index=True)
        transitions.to_csv('mr_{}_in_{}_{}_transitions.csv'.format(day, name, suffix))
        transitions_agg.to_csv('mr_{}_in_{}_{}_transitions_agg.csv'.format(day, name, suffix))
        confidence.to_csv('mr_{}_in_{}_{}_confidence.csv'.format(day, name, suffix))
        end = time.time()
        logging.info('Mined processes for {} in {}'.format(day, end-start))
        print('Mined processes for {} in {}'.format(day, end-start))
    return global_dependencies_l, global_confidences_l


# Data Frames
contracts_lookup = pd.read_csv(path_contracts_lookup)
block_times = pd.read_csv(path_block_times)

if not check_data_fame_conformance(contracts_lookup, contracts_lookup_columns):
    raise SyntaxError('The column names of the contracts lookup csv file do no match the required colum names: {}'.format(contracts_lookup_columns))

if not check_data_fame_conformance(block_times, block_times_columns):
    raise SyntaxError('The column names of the block times lookup csv file do no match the required colum names: {}'.format(block_times_columns))

os.chdir(path_to_raw_transaction_bulk)

global_dependencies = pd.DataFrame(columns=['day', 'CtC->CtC', 'CtC->CtU', 'CtC->UtU', 'CtC->end', 'CtU->CtC', 'CtU->CtU',
       'CtU->UtC', 'CtU->UtU', 'CtU->end', 'UtC->CtC', 'UtC->CtU', 'UtC->UtC',
       'UtC->UtU', 'UtC->end', 'UtU->CtC', 'UtU->CtU', 'UtU->UtC', 'UtU->UtU',
       'UtU->end', 'sta->CtC', 'sta->UtC', 'sta->UtU'])
global_confidences = pd.DataFrame(columns=['day', 'CtC->CtC', 'CtC->CtU', 'CtC->UtU', 'CtC->end', 'CtU->CtC', 'CtU->CtU',
       'CtU->UtC', 'CtU->UtU', 'CtU->end', 'UtC->CtC', 'UtC->CtU', 'UtC->UtC',
       'UtC->UtU', 'UtC->end', 'UtU->CtC', 'UtU->CtU', 'UtU->UtC', 'UtU->UtU',
       'UtU->end', 'sta->CtC', 'sta->UtC', 'sta->UtU'])

raw_transaction_candidates = [i for i in glob.glob('*.{}'.format(extension))]
j = 0

for candidate_path in raw_transaction_candidates:
    path_head, path_tail = ntpath.split(candidate_path)
    file_infix = path_tail[:-4]
    raw_transactions = pd.read_csv(candidate_path)
    if not check_data_fame_conformance(raw_transactions, raw_transaction_columns):
        logging.info('File {} not matching the raw transaction pattern. Skipping now'.format(candidate_path))
        print('File {} not matching the raw transaction pattern. Skipping now'.format(candidate_path))
    else:
        logging.info('Provided file {} matches the raw transaction pattern. Applying now parsing operation'.format(
            candidate_path))
        print('Provided file {} matches the raw transaction pattern. Applying now parsing operation'.format(
            candidate_path))

        parsing_start = time.time()

        addresses_lookup, transaction_hashes, events = parser.parse_event_log(raw_transactions, contracts_lookup,
                                                                              block_times)
        addresses_lookup.to_csv('ps_{}_address_lookup.csv'.format(file_infix))
        transaction_hashes.to_csv('ps_{}_transaction_lookup.csv'.format(file_infix))
        events.to_csv('ps_{}_event_log.csv'.format(file_infix))

        parsing_end = time.time()

        logging.info('Parsed {} in {}s'.format(file_infix, parsing_end - parsing_start))
        print('Parsed {} in {}s'.format(file_infix, parsing_end - parsing_start))

        mining_start = time.time()

        logging.info('Starting now mining operation for {}'.format(file_infix))
        print('Starting now mining operation for {}'.format(file_infix))
        events['day'] = pd.to_datetime(events['timestamp'], unit='s').dt.normalize()

        dep, con = mine_segment_by_day(events, file_infix, j)
        global_dependencies = global_dependencies.append(dep)
        global_confidences = global_confidences.append(con)

        global_dependencies = global_dependencies.fillna(0)
        global_dependencies['total_events'] = global_dependencies.sum(axis=1)

        reduced_global_dependencies = global_dependencies.groupby('day').sum()

        min_day = reduced_global_dependencies.index.min()
        max_day = reduced_global_dependencies.index.max()
        rgd_filename = 'rgd_{}_to_{}.csv'.format(str(min_day)[0:10], str(max_day)[0:10])
        reduced_global_dependencies.to_csv(rgd_filename)
        j += 1

