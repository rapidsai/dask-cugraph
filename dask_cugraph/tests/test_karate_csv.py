import operator
from dask.distributed import Client, wait, default_client, futures_of
from dask_cuda import LocalCUDACluster
import gc
from itertools import product
import time
import numpy as np

import pytest
# Temporarily suppress warnings till networkX fixes deprecation warnings
# (Using or importing the ABCs from 'collections' instead of from
# 'collections.abc' is deprecated, and in 3.8 it will stop working) for
# python 3.7.  Also, this import networkx needs to be relocated in the
# third-party group once this gets fixed.
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import networkx as nx

def test_pagerank():
    gc.collect()
    input_data_path = r"datasets/karate.csv"
    import pandas as pd
    pd_df = pd.read_csv(input_data_path, delimiter=' ', names=['src', 'dst', 'value'])
    import networkx as nx
    G = nx.Graph()
    for i in range(0,len(pd_df)):
        G.add_edge(pd_df['src'][i],pd_df['dst'][i])
    nx_pr = nx.pagerank(G, alpha=0.85)
    nx_pr = sorted(nx_pr.items(), key=lambda x: x[0])
    print(nx_pr)

    cluster = LocalCUDACluster(threads_per_worker=1)
    client = Client(cluster)
    import numpy as np
    import cudf
    import dask_cudf
    import dask_cugraph.pagerank as dcg
    input_df = cudf.DataFrame()
    print("Read Input Data.")
    input_data_path = r"datasets/karate.csv"
    ddf = dcg.read_csv(input_data_path, delimiter=' ', names=['src', 'dst', 'value'], dtype=['int32', 'int32', 'float32'])
    print("CALLING DASK MG PAGERANK")
    pr = dcg.mg_pagerank(ddf)
    print(pr)
    res_df = pr.compute()
    #res_df = f_pr.sort_values('pagerank')
    #print(len(res_df))
    for c in range(len(res_df)):
        print(res_df['vertex'][c], res_df['pagerank'][c])

    assert 1==0
