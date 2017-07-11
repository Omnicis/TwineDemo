# Test Setup

Hardware: Mac Air, i7, 8G

LungCancer code SHA: 356cebc32eb03de6cdebfc24b2365d7af56ecad5
SMV code SHA: 8d7eb652e3f27e4d9caa8c24475082471db08f11

Spark binary: Spark 1.3.0 (git revision 908a0bf) built for Hadoop 1.0.4

Spark conf:
```
spark.driver.memory              4g
spark.executor.memory            4g
```

Command:

```
$ ./shell/run.sh data
```

# Result

## With KryoSerializer

```scala
scala> val b=s(pdda.cci.NatIncRateSeer)
15/08/21 09:29:14 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/08/21 09:29:14 WARN LoadSnappy: Snappy native library not loaded
09:29:16 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerRaw_e4912903.csv
09:36:18 RunTime: 7 minutes, 1 second and 776 milliseconds
09:36:18 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopGroup_21c34050.csv
09:37:22 RunTime: 1 minute, 4 seconds and 148 milliseconds
09:37:22 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopByCounty_b1cd8641.csv
09:40:12 RunTime: 2 minutes, 50 seconds and 152 milliseconds
09:40:13 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerByCounty_46c73c05.csv
09:42:12 RunTime: 1 minute, 59 seconds and 59 milliseconds
09:42:12 PERSISTING: data/output/com.omnicis.mag.pdda.cci.CntyIncRateSeer_aabe7acd.csv
09:42:26 RunTime: 14 seconds and 625 milliseconds
09:42:27 PERSISTING: data/output/com.omnicis.mag.pdda.cci.NatIncRateSeer_aea48eb5.csv
09:42:30 RunTime: 3 seconds and 948 milliseconds
```

**A rerun of this can NOT repeat the result. The new result is similar to the other 2 runs!**

## Without KryoSerializer

```scala
scala> val b=s(pdda.cci.NatIncRateSeer)
15/08/21 09:46:04 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/08/21 09:46:04 WARN LoadSnappy: Snappy native library not loaded
09:46:06 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerRaw_e4912903.csv
09:52:01 RunTime: 5 minutes, 54 seconds and 985 milliseconds
09:52:01 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopGroup_21c34050.csv
09:52:53 RunTime: 51 seconds and 758 milliseconds
09:52:53 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopByCounty_b1cd8641.csv
09:55:15 RunTime: 2 minutes, 22 seconds and 19 milliseconds
09:55:15 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerByCounty_46c73c05.csv
09:56:58 RunTime: 1 minute, 43 seconds and 285 milliseconds
09:56:58 PERSISTING: data/output/com.omnicis.mag.pdda.cci.CntyIncRateSeer_aabe7acd.csv
09:57:12 RunTime: 13 seconds and 682 milliseconds
09:57:12 PERSISTING: data/output/com.omnicis.mag.pdda.cci.NatIncRateSeer_aea48eb5.csv
09:57:15 RunTime: 2 seconds and 892 milliseconds
```

## With KryoSerializer and Registered all SMV Serializable classes

```scala
scala> val b=s(pdda.cci.NatIncRateSeer)
15/08/21 11:17:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/08/21 11:17:24 WARN LoadSnappy: Snappy native library not loaded
11:17:25 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerRaw_e4912903.csv
11:22:59 RunTime: 5 minutes, 33 seconds and 612 milliseconds
11:22:59 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopGroup_21c34050.csv
11:23:51 RunTime: 52 seconds and 285 milliseconds
11:23:51 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerPopByCounty_b1cd8641.csv
11:26:14 RunTime: 2 minutes, 22 seconds and 257 milliseconds
11:26:14 PERSISTING: data/output/com.omnicis.mag.pdda.etl.SeerByCounty_46c73c05.csv
11:27:56 RunTime: 1 minute, 42 seconds and 609 milliseconds
11:27:56 PERSISTING: data/output/com.omnicis.mag.pdda.cci.CntyIncRateSeer_aabe7acd.csv
11:28:10 RunTime: 13 seconds and 435 milliseconds
11:28:10 PERSISTING: data/output/com.omnicis.mag.pdda.cci.NatIncRateSeer_aea48eb5.csv
11:28:13 RunTime: 2 seconds and 879 milliseconds
```

# Conclusion

Turn on Kryo from spark-default.conf does not improve performance on a simple app (not persisting actions). Register SMV classes or not also does not impact performance.
