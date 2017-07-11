from smv import *
from smv.error import SmvRuntimeError
from pyspark.sql import DataFrame, Column, Window

from pyspark.sql.functions import *

try:
    import pandas as pd
except ImportError:
    pass
else:
    _pandas_max_columns_ = 100
    pd.set_option("display.max_columns", _pandas_max_columns_)

    def jshow(df, n=10):
        """Same as DataFrame's `show()` method, but use pandas to show the data as HTML table

            Args:
                n (int): number of rows to show

            Example:
                >>> df1.jshow(20)

            Returns:
                (None):
        """
        return df.limit(n).toPandas().head(n)

    DataFrame.jshow = jshow


def get_median(smvgd, valcol):
    """Calculated median on a DF or SmvGroupedData of given column "valcol"

        When the total number in a group is an odd number, median is defined as the member
        ranked in the exact middle. When the total number in a group is an even number,
        median is defined as the average of the 2 member values ranked in the middle.

        Args:
            smvgd (DataFrame or SmvGroupedData): input data
            valcol (Column or str): column to calculate median on

        Return:
            (DataFrame): result DataFrame with
                * key columns from the input SmvGroupedData
                * median column (with name "median")
    """
    if (isinstance(smvgd, SmvGroupedData)):
        df = smvgd.df
        keys = smvgd.keys
    elif (isinstance(smvgd, DataFrame)):
        df = smvgd
        keys = []
    else:
        raise RuntimeError("get_median method takes DataFrame or SmvGroupedData as its first parameter")

    if (isinstance(valcol, Column)):
        vc = valcol
    elif (isinstance(valcol, basestring)):
        vc = col(valcol)
    else:
        raise RuntimeError('get_median methods takes string or Column as its second parameter')

    # Only count the non-null vc
    total = df.groupBy(*keys).agg(count(vc).alias("n"))

    w = Window.partitionBy(*keys).orderBy(vc)

    df2 = df.select(keys + [vc]).where(vc.isNotNull()
        ).smvJoinByKey(total, keys, 'inner'
        ).withColumn('rank', rowNumber().over(w)
        ).withColumn('selInd',
            when(
                ((col('n') % 2 == 1) & (col('rank') * 2 == col('n') + 1)) |
                ((col('n') % 2 == 0) & (col('rank').isin(col('n') / 2, col('n') / 2 + 1))),
            lit(1)).otherwise(lit(0))
        )

    return df2.where(col('selInd') == 1
        ).groupBy(*keys).agg(
            mean(vc).alias('median')
        )


class ClaimUtils(object):
    """Util methods for Claim Data

        Args:
            _colMap (dictionary): Claim data column name mapping

        Example:
            code::

                claimUtils = ClaimUtils({
                    "ptnt_id": "ptnt_gid",
                    "phyn_id": "phyn_gid",
                    "clm_id":  "clm_gid",
                    "dt": "dt"
                })
    """
    def __init__(self, _colMap):
        self.colMap = _colMap

    def _check_cols(self, df, *cols):
        for c in cols:
            if not (c in self.colMap):
                raise SmvRuntimeError("Column key {0} is not defined in the colMap.".format(c))
            else:
                colName = self.colMap.get(c)
                if not (colName in df.columns):
                    raise SmvRuntimeError("Column {0} does not exist in the given DataFrame".format(colName))

    def toPtntPrimPhyn(self, df):
        """For each Ptnt identify the primary physician

           Args:
               df (DataFrame): The claim data frame which has the required input columns

           Returns:
               (DataFrame): The result data frame with 2 columns: ptnt_id & prim_phyn_id

           Expected input:
             * ptnt_id
             * phyn_id
             * clm_id
             * dt
        """
        self._check_cols(df, "ptnt_id", "phyn_id", "clm_id", "dt")

        c = self.colMap
        cc = lambda colid: col(c[colid])

        #TODO: use a "tmp_col_name" method
        return df.groupBy(c['ptnt_id'], c['phyn_id']).agg(
            countDistinct(c['clm_id']).alias('clm_count'),
            max(c['dt']).alias('last_dt')
        ).smvDedupByKeyWithOrder(cc('ptnt_id')
        )(col('clm_count').desc(), col('last_dt').desc()
        ).select(
            cc('ptnt_id'),
            cc('phyn_id').alias('prim_' + c['phyn_id'])
        )

    def ptntSubtypeSummary(self, df, subColName):
        """Return DF at ptnt-subtype level, with

            * first_dt
            * last_dt
            * clm_count
        """
        self._check_cols(df, 'ptnt_id', 'dt', 'clm_id')

        c = self.colMap

        return df.groupBy(c['ptnt_id'], subColName).agg(
            min(c['dt']).alias('first_dt'),
            max(c['dt']).alias('last_dt'),
            countDistinct(c['clm_id']).alias('clm_count')
        )

    def ptntSubtypePivot(self, df, subColName, baseOutput):
        """Return DF at ptnt level with

            * first_dt
            * last_dt
            * clm_count

            pivoted by `subColName`
        """
        self._check_cols(df, 'ptnt_id', 'dt', 'clm_id')

        c = self.colMap

        return self.ptntSubtypeSummary(df, subColName)\
            .smvGroupBy(c['ptnt_id']).smvPivotCoalesce(
                [[subColName]], ['first_dt', 'last_dt', 'clm_count'], baseOutput
            )
