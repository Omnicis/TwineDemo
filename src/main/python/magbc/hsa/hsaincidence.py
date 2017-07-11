from smv import *
from pyspark.sql.functions import *

import inputdata
from magbc.core import *

class HSAIncidence(SmvModule):
    """ Calculate incidence rates and screening rates for all age groups on hsa hrr level"""

    def requiresDS(self):
        return [
            inputdata.IncStateBreastCancerProfile,
            inputdata.ScreeningStateBreastCancerProfile,
            inputdata.ZipMaster
        ]

    def run(self, i):
        df = i[inputdata.IncStateBreastCancerProfile]
        dfs = i[inputdata.ScreeningStateBreastCancerProfile]
        zipM = i[inputdata.ZipMaster]

        zipMfiltered = zipM.where(
            col('hsanum').isNotNull()
        ).select(
            'hsanum',
            'hrrnum',
            'pop',
            col('fips').alias('FIPS')
        )

        scp = df.select(
            (coalesce('Average Annual Count', lit(0.0))).cast('double').alias('Annual_Incidence_Rate'),
            'FIPS'
        )

        scr = dfs.select(
            'FIPS',
            (col('Model-Based Percent(3)') / lit(100.0)).alias('Screening_Rate')
        )

        res = scp.smvJoinByKey(zipMfiltered, ['FIPS'], 'inner'
        ).smvJoinByKey(scr, ['FIPS'], 'inner'
        ).groupBy('hsanum', 'hrrnum').agg(
            scaleRate(col('Annual_Incidence_Rate'), col('pop')).alias('AllAge_incRate'),
            scaleRate(col('Screening_Rate'), col('pop')).alias('AllAge_scrRate'),
            sum(col('pop')).alias('pop')
        )

        return res
