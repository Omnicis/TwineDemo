from smv import *
from smv.functions import *
from pyspark.sql.functions import *
import inputdata
from magbc.core import hier_utils
from magbc.core import targetDrugList
from twinelib.hier import HierarchyUtils

class ZipHierMap(SmvModule):
    """
    Standardize ZipMaster for hierarchy aggregation
    """

    def requiresDS(self):
        return [inputdata.ZipMaster]

    def run(self, i):
        z = i[inputdata.ZipMaster]
        return z.select(
            'zip',
            z.fips.alias('county'),
            'county_name',
            z.State.alias('state'),
            z.StateName.alias('state_name'),
            z.hsanum.alias('hsa'),
            concat_ws(", ", z.hsacity, z.hsastate).alias('hsa_name'),
            z.hrrnum.alias('hrr'),
            concat_ws(", ", z.hrrcity, z.hrrstate).alias('hrr_name'),
            lit('US').alias('country'),
            lit('US').alias('country_name')
        )
    def isEphemeral(self):
        return False


class AllStatsGeoLevel(SmvModule, SmvOutput):
    """
    Aggregate Physician level stats to hierarchical geo level
    """
    def requiresDS(self):
        return[ZipHierMap, 
        inputdata.AllPhynStats]

    def run(self, i):
        zipmap = i[ZipHierMap]
        stats = i[inputdata.AllPhynStats].\
        withColumn("zip", col("phyn_zip_cde").substr(1, 5))

        stats_agged = hier_utils.hier_agg(
            stats,
            zipmap,
            'hsa', 'hrr', 'country'
        )(*([sum(lit(1)).alias("Number_of_Physicians")] +
            [sum(col("NumberOfPrescriptions_" +r)
                ).alias("NumberOfPrescriptions_" +r) for r in targetDrugList] +
            [sum(col("Total_Amount_of_Payment_" +r)
                ).alias("Total_Amount_of_Payment_" +r) for r in targetDrugList]
            ))
        return stats_agged
