import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

data = pd.read_csv('update_localhost.csv')
reqeust_count = data['request']
sequential_result = data['sequential']
concurrent_result = data['concurrent']
bulk_result = data['bulk']


# plt.title('update document(localhost)')
x_pos = np.arange(len(reqeust_count))
plt.xticks(x_pos, reqeust_count)
plt.xlabel('batch size')
plt.ylabel('time (ms)')
plt.bar(x_pos-0.2, sequential_result, width=0.2, label='sequential')
plt.bar(x_pos, concurrent_result, width=0.2, label='concurrent')
plt.bar(x_pos+0.2, bulk_result, width=0.2, label='bulk')
# log scale
plt.yscale('log')
plt.legend()
plt.show()



data = pd.read_csv('update_remote.csv')[:-1]
reqeust_count = data['request']
sequential_result = data['sequential']
concurrent_result = data['concurrent']
bulk_result = data['bulk']


# plt.title('update document(remote)')
x_pos = np.arange(len(reqeust_count))
plt.xticks(x_pos, reqeust_count)
plt.xlabel('batch size')
plt.ylabel('time (ms)')
plt.bar(x_pos-0.2, sequential_result, width=0.2, label='sequential')
plt.bar(x_pos, concurrent_result, width=0.2, label='concurrent')
plt.bar(x_pos+0.2, bulk_result, width=0.2, label='bulk')
# log scale
plt.yscale('log')
plt.legend()
plt.show()

data = pd.read_csv('partial_localhost.csv')
reqeust_count = data['keys']
full_result = data['fullRecords']
partial_result = data['partialRecords']


# plt.title('partial records(localhost)')
x_pos = np.arange(len(reqeust_count))
plt.xticks(x_pos, reqeust_count)
plt.xlabel('document number')
plt.ylabel('time (ms)')
plt.bar(x_pos-0.2, full_result, width=0.2, label='fullRecords')
plt.bar(x_pos, partial_result, width=0.2, label='partialRecords')
# log scale
plt.yscale('log')
plt.legend()
plt.show()

