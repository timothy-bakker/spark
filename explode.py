from pyspark.sql.types import *
from pyspark.sql.functions import *
from itertools import chain

def to_explode(df, by):
  cols, dtypes = zip(*((c,t) for (c, t) in df.dtypes if c not in by))
  # Spark SQL supports only homogeneous columns
  assert len(set(dtypes))==1,"All columns have to be of the same type"
  # Create and explode an array of (column_name, column_value) structs
  kvs = explode(array([
    struct(lit(c).alias("key"), col(c).alias("val")) for c in cols
  ])).alias("kvs")
  
  return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])
