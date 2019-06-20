import pytest
import pandas as pd
from dask.distributed import Client, wait, default_client, futures_of
from dask_cuda import LocalCUDACluster
import os


def test_one_csv_file_pagerank():
    cluster = LocalCUDACluster(threads_per_worker=1)
    client = Client(cluster)

    import dask.dataframe as dd
    import dask_cudf
    from toolz import first, assoc
    import cudf
    import numba.cuda as cuda
    import numpy as np

    import pandas.testing

    import dask_cugraph.pagerank as dcg


    print("Read Input Data.")
    input_data_path = r"/datasets/pagerank_demo/1/Input-huge/edges/temp/part-00000.csv"
    ddf = dcg.read_csv(input_data_path, delimiter='\t', names=['src', 'dst'], dtype=['int32', 'int32'])
    print("DASK CUDF: ", ddf)
    print("CALLING DASK MG PAGERANK")

    pr = dcg.mg_pagerank(ddf)
    #print(pr)
    #cu_df = pr.compute()
    #res_df = cu_df.sort_values('pagerank')
    #print("RESULT:")
    #print(res_df)
    '''pd_df = pd.DataFrame({'vertex':np.ones(len(x), dtype =int),'pagerank':x + y})
    exp_df = pd_df.sort_values(by=['pagerank'])
    print(exp_df)
    pd.util.testing.assert_frame_equal(res_df.to_pandas().reset_index(drop=True), exp_df.reset_index(drop=True))
    '''
    client.close()


def test_multiple_csv_file_pagerank():
    cluster = LocalCUDACluster(threads_per_worker=1)
    client = Client(cluster)

    import dask.dataframe as dd
    import dask_cudf
    from toolz import first, assoc
    import cudf
    import numba.cuda as cuda
    import numpy as np

    import pandas.testing

    import dask_cugraph.pagerank as dcg


    print("Read Input Data.")
    input_data_path = r"/datasets/pagerank_demo/8/Input-huge/edges/temp"
    ddf = dcg.read_csv(input_data_path + r"/part-*", delimiter='\t', names=['src', 'dst'], dtype=['int32', 'int32'])
    print("DASK CUDF: ", ddf)
    print("CALLING DASK MG PAGERANK")

    pr = dcg.mg_pagerank(ddf)
    #print(pr)
    #cu_df = pr.compute()
    #res_df = cu_df.sort_values('pagerank')
    #print("RESULT:")
    #print(res_df)
    '''pd_df = pd.DataFrame({'vertex':np.ones(len(x), dtype =int),'pagerank':x + y})
    exp_df = pd_df.sort_values(by=['pagerank'])
    print(exp_df)
    pd.util.testing.assert_frame_equal(res_df.to_pandas().reset_index(drop=True), exp_df.reset_index(drop=True))
    '''
    client.close()

