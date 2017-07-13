###---GLOBAL_IMPORTS_START---###
from smv import *
from pyspark.sql.functions import *
###---GLOBAL_IMPORTS_END---###
###---ZipHierMap_IMPORTS_START---###
import magbc.geo.inputdata


###---ZipHierMap_IMPORTS_END---###
from smv import *
from smv.functions import *
from pyspark.sql.functions import *
import inputdata
from magbc.core import hier_utils, hier_keys
from twinelib.hier import HierarchyUtils

class ZipHierMap(SmvModule, SmvOutput):
    """
    Standardize ZipMaster for hierarchy aggregation
    """

    def requiresDS(self):
        return [magbc.geo.inputdata.ZipMaster]

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
