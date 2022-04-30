import os
os.environ['PYTHONHASHSEED']="0"
os.environ["PYSPARK_PYTHON"]="python3"
#os.environ["JAVA_HOME"]="/usr/lib/jvm/java-8-openjdk-amd64/"
# A few additional libraries we will need

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

try:
  conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "1g").set("spark.executorEnv.PYTHONHASHSEED","0").set("spark.ui.port", "4050")
  sc = SparkContext(conf = conf)
  spark = SparkSession.builder.getOrCreate()
except ValueError:
  #it's ok if the server is already started
  pass
def dbg(x):
  """ A helper function to print debugging information on RDDs """
  if isinstance(x, pyspark.RDD):
    print([(t[0], list(t[1]) if 
            isinstance(t[1], pyspark.resultiterable.ResultIterable) else t[1])
           if isinstance(t, tuple) else t
           for t in x.take(100)])
  else:
    print(x)
    
# Unittest init.
import unittest
Test = unittest.TestCase()