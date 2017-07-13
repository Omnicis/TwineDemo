from smv import *
from smv.error import SmvRuntimeError
from pyspark.sql.functions import *

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
