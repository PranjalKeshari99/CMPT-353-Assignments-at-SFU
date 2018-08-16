import numpy as np
import pandas as pd
import time
from implementations import all_implementations



data = pd.DataFrame(columns=['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'], index=np.arange(100))

for i in range(100):
	random_array = np.random.randint(-500,500,1000)
	for sort in all_implementations:
		st = time.time()
		res = sort(random_array)
		en = time.time()
		tot = en-st
		data.iloc[i][sort.__name__] = tot


data.to_csv('data.csv', index=False)

