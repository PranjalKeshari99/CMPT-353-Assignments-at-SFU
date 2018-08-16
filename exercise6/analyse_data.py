import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import sys

input_file = sys.argv[1]

data = pd.read_csv(input_file)

anova = stats.f_oneway(data['qs1'], data['qs2'], data['qs3'], data['qs4'], data['qs5'], data['merge1'], data['partition_sort'])
#p < 0.05 so yes there is a difference between the means of the groups

data_melt = pd.melt(data)
data_melt['value'] = data_melt['value'].astype('float64')

posthoc = pairwise_tukeyhsd(
    data_melt['value'], data_melt['variable'],
    alpha=0.05)

print(posthoc)

fig = posthoc.plot_simultaneous()