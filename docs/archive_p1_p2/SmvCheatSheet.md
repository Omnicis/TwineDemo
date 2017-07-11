## Use Spark Shell with SMV (To be updated)
Before starting the shell, you need to compile the project first. Under the
`LungCancer` project directory, run maven:
```shell
$ mvn package
```
On some machine, the command is `mvn3`

### Start Spark Shell
Please check the "shell/run.sh" code and modify it with your machine's paths.

Under `LungCancer` project directory, start the interactive shell
```shell
$ ./shell/run.sh data
```
Here I assume you have data directory under the Project directory.

### Load a SmvModule
You can access all project modules under the packages listed at the beginning of
`shell/shell_init.pdda.scala` file.
```scala
import com.omnicis.pdda._, core._ , etl._, adhoc._
```

If you go through other parts of the init file, you can find that function `s()` will
resolve the SchemaRDD (DataFrame). Here is a quick example:

```scala
val d1=s(Chr2014)
d1.printSchema
```
It will show the schema of `Chr2014`.

Since `d1` now is a `DataFrame` of Spark, you can use all the DF functions such as `select`,
`where`, `join`, `groupBy/agg`, etc. You can also use
[SMV extended DF functions](https://github.com/TresAmigosSD/SMV/blob/to1.3/docs/DF_Functions.md),
Including `selectPlus`, `selectMinus`, `renameField`, `joinByKey`, `smvPivot`, `smvCube`, `smvRollup`,
etc.

## EDD in shell
Extended Data Dictionary is very useful with the interactive shell. You can run basic statistics
and even histograms with the data in the shell.

Please refer [SMV EDD document](https://github.com/TresAmigosSD/SMV/blob/to1.3/docs/Edd.md) for details.

### Base statistics
```scala
scala> val ref = s(p.referral)
scala> ref.limit(1000).edd.addBaseTasks().dump
```
where `p` is defined in the init file as
```scala
val p = PddaApp
```
for easier typing.

### Histogram
```scala
scala> ref.limit(1000).edd.addHistogramTasks("Pair_Count")(binSize = 10).dump
```
### Save to file
```scala
scala> ref.limit(1000).edd.addHistogramTasks("Pair_Count")(binSize = 10).saveReport("output/test1.hist")
```
Here the "path" parameter of `saveReport` method is relevant to the project directory.

## Draw map from shell (C9 dependency)
### Create Csv file from shell
```scala
scala> val chr=s(Chr2014)
scala> chr.select('fips, 'High_school_graduation_Value).savel("output/test.csv")
```
Now you have the "test.csv" under "LungCancer/output". Under c9 IDE, right click on that file name on the left navigation tree, select "Open on US Map". You should have "high school graduation rate" plotted on the map. Now move the map tab to another panel either to the right or under the shell tab. Get back to the shell can try this
```scala
scala> chr.select('fips, 'Children_eligible_for_free_lunch_Value).savel("output/test.csv")
```
The map should change about 0.5 second after the command.
