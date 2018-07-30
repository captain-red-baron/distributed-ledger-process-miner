import os
import ntpath
import glob
import time

import pandas as pd
import numpy as np

import logging
logging.basicConfig(filename='address_shortner.log',level=logging.DEBUG)

pd.options.mode.chained_assignment = None

path_to_raw_transaction_bulk = '/Users/marcelmuller/Documents/Uni/Master/Semester_9_SS_18/Masterarbeit/parity_data'

sub_dir_dept = 1

