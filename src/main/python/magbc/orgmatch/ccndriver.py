from smv import *
from pyspark.sql.functions import *

import inputdata

class CcnDriver(SmvModule):
    """Get list of ccn accounts"""
    def requiresDS(self):
        return [
            inputdata.HospHSA,
            inputdata.CmsHCMeasure
        ]

    def run(self, i):
        df = i[inputdata.HospHSA]
        hc = i[inputdata.CmsHCMeasure]

        hosp = df.select(
            col('CCN'),
            'hospital',
            'hospcity',
            'hospstate',
            'hsanum',
            'hrrnum'
        )

        hcdf = hc.select(
            col('Provider_ID').alias('CCN'),
            'Hospital_Name',
            'Address',
            'City',
            'State',
            'Zip_Code'
        )

        res = hosp.smvJoinByKey(hcdf, ['CCN'], 'leftouter').select(
            'CCN',
            coalesce('Hospital_Name', 'hospital').alias('name'),
            coalesce('City', 'hospcity').alias('city'),
            coalesce('State', 'hospstate').alias('state'),
            col('Address').alias('address'),
            col('Zip_Code').alias('zip'),
            'hsanum',
            'hrrnum'
        )

        return res
