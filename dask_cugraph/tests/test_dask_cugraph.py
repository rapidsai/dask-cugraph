import pytest
import pandas as pd
from dask.distributed import Client, wait, default_client, futures_of
from dask_cuda import LocalCUDACluster


def test_pagerank():
    cluster = LocalCUDACluster(n_workers = 4, threads_per_worker=1)
    client = Client(cluster)

    import dask.dataframe as dd
    import dask_cudf
    from toolz import first, assoc
    import cudf
    import numba.cuda as cuda
    import numpy as np

    import pandas.testing

    import dask_cugraph.pagerank as dcg


    input_df = cudf.DataFrame()
    x = np.asarray([0,1,2,3,6,25,0,1,8,20,27,1,2,5,7,8,28,2,3,4,11,2,4,22,26,0,5,15,2,6,13,20,30,0,7,11,16,26,6,8,9,12,18,22,26,0,9,10,20,22,24,26,1,10,14,17,28,5,11,23,10,12,2,0,1,14,19,3,15,21,3,15,16,5,9,17,19,29,0,18,25,7,15,19,2,20,31,10,21,1,16,20,22,11,23,25,5,14,17,23,24,12,17,21,25,4,23,25,26,8,27,2,4,26,28,31,11,16,22,29,12,13,30,23,27,31],dtype=np.int32)
    y = np.asarray([0,0,0,0,0,0,1,1,1,1,1,2,2,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,6,6,6,6,6,7,7,7,7,7,8,8,8,8,8,8,8,9,9,9,9,9,9,9,10,10,10,10,10,11,11,11,12,12,13,14,14,14,14,15,15,15,16,16,16,17,17,17,17,17,18,18,18,19,19,19,20,20,20,21,21,22,22,22,22,23,23,23,24,24,24,24,24,25,25,25,25,26,26,26,26,27,27,28,28,28,28,28,29,29,29,29,30,30,30,31,31,31], dtype = np.int32)
    input_df['src']=x
    input_df['dst']=y

    ddf = dask_cudf.from_cudf(input_df, npartitions=4).persist()
    print("DASK CUDF: ", ddf)
    print("CALLING DASK MG PAGERANK")

    pr = dcg.mg_pagerank(ddf)
    print(pr)
    res_df = pr.compute()
    print("RESULT:")
    print(res_df)
    exp_df = pd.DataFrame({'pagerank':x + y})
    print(exp_df)
    pd.util.testing.assert_frame_equal(res_df.to_pandas(), exp_df)
    
    client.close()
