from smv import *
from pyspark.sql.functions import *

import inputdata

class HSACapacity(SmvModule):
    """clean up columns from Capacity file"""
    def requiresDS(self):
        return [inputdata.Capacity]

    def run(self, i):
        df = i[inputdata.Capacity]

        return df.smvSelectPlus(
            regexp_replace(col('Resident_Population_2010'), ',', '').cast('float').alias('_Resident_Population_2010')
        ).drop('Resident_Population_2010'
        ).smvRenameField(
            ('_Resident_Population_2010', 'Resident_Population_2010'),
            ("HSA", "hsanum")
        )
