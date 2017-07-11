from smv import *
from pyspark.sql.functions import *

import inputdata
from magbc.core import *

class HSAUninsured(SmvModule):
    """calculate rate uninsured on hsa level"""
    def requiresDS(self):
        return [
            inputdata.ZipMaster,
            inputdata.Uninsured
        ]

    def run(self, i):
        df = i[inputdata.Uninsured].smvRenameField(('county_fips', 'FIPS'))
        zipM = i[inputdata.ZipMaster].smvRenameField(('fips', 'FIPS'))

        joined = zipM.smvJoinByKey(df, ['FIPS'], 'inner')

        res = joined.groupBy('hsanum').agg(
            #could extract as a function
            scaleRate(col('uninsured14pct')/lit(100), col('pop')).alias('Uninsured_Rate')
        )

        return res

class HSAUnderinsured(SmvModule):
    """ Calculate underinsured and medicaid rates on hsa level"""
    def requiresDS(self):
        return [
            inputdata.ZipMaster,
            inputdata.Underinsured
        ]

    def run(self, i):
        zipM = i[inputdata.ZipMaster]
        df = i[inputdata.Underinsured]

        dfState = df.where(col('geo_type') == 'State').smvRenameField(('geo_value', 'State'))

        popdf = zipM.groupBy('State').agg(sum(col('pop')).alias('state_pop'))

        joined = zipM.smvJoinByKey(dfState, ['State'], 'inner'
        ).smvJoinByKey(popdf, ['State'], 'inner').smvSelectPlus(
            (col('Medicaid_2013') / col('state_pop')).alias('Medicaid_Rate')
        )

        return joined.groupBy('hsanum').agg(
            scaleRate(col('uninsured_or_underinsured_rate_2012'), col('pop')).alias('Underinsured_Rate'),
            scaleRate(col('Medicaid_Rate'), col('pop')).alias('Medicaid_Rate')
        )
