from setuptools import setup
import os 
os.system("git clone https://github.com/rapidsai/dask-cudf.git ../dask-cudf")
os.system("cd ../dask-cudf && pip install .")


packages = ["dask_cugraph",
   "dask_cugraph.pagerank"]
install_requires = [
  'numpy'
]

setup(
   name='dask_cugraph',
   version='1.0',
   description='',
   author='NVIDIA Corporation',
   packages=packages,
   install_requires=install_requires
)



