import pytest
import pandas as pd
from dask.distributed import Client
from dask_cuda import LocalCUDACluster
import dask.dataframe as dd
import dask_cudf
import pandas.testing

import dask_cugraph.pagerank as dcg

def test_one_csv_file_pagerank():
    cluster = LocalCUDACluster(threads_per_worker=1)
    client = Client(cluster)

    print("Read Input Data.")
    input_data_path = r"datasets/hibench_small/1/part-00000.csv"
    chunksize = dcg.get_chunksize(input_data_path)
    ddf = dask_cudf.read_csv(input_data_path, chunksize = chunksize,  delimiter='\t', names=['src', 'dst'], dtype=['int32', 'int32'])
    print("DASK CUDF: ", ddf)
    print("CALLING DASK MG PAGERANK")

    pr = dcg.pagerank(ddf)
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

    print("Read Input Data.")
    input_data_path = r"datasets/hibench_small/2/"
    chunksize = dcg.get_chunksize(input_data_path + r"/part-*")
    ddf = dask_cudf.read_csv(input_data_path + r"/part-*", chunksize = chunksize, delimiter='\t', names=['src', 'dst'], dtype=['int32', 'int32'])
    print("DASK CUDF: ", ddf)
    print("CALLING DASK MG PAGERANK")

    pr = dcg.pagerank(ddf)
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

