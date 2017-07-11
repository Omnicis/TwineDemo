"""
Project level shared functions (Driver side functions only) and constants
"""
import abc
from smv import SmvHiveTable
from smv.runconfig import SmvRunConfig
from pyspark.sql.functions import *

from twinelib.utils import ClaimUtils
from twinelib.hier import HierarchyUtils

def do_sample(df, column_name, level):
    """Sample DF
    `level` can have 4 values:
      * full - No sampling
      * "hash5pct" - 5 percent Hash Sampling on sku_number
      * "hash1pct" - 1 percent Hash Sampling on sku_number
      * "hash0.1pct" - 0.1 percent Hash Sampling on sku_number
      * "hash0.2pct" - 0.2 percent Hash Sampling on sku_number
    """

    if (level == "hash5pct"):
        # smvHashSample(keyname, rate, seed=23) You can specify other
        # seed to get a different set of 5 percent.
        # Hash sample guarantees that is an "sku_number" is selected
        # in one DF, the records belongs to the same "sku_number" will
        # be selected in other DFs.
        return df.smvHashSample(column_name, 0.05)
    elif (level == "hash1pct"):
        return df.smvHashSample(column_name, 0.01)
    elif (level == "hash0.1pct"):
        return df.smvHashSample(column_name, 0.001)
    elif (level == "hash0.2pct"):
        return df.smvHashSample(column_name, 0.002)
    elif (level == "full"):
        return df
    else:
        raise NotImplementedError


class GSonarHiveTable(SmvHiveTable, SmvRunConfig):
    """Base class for Hive tables from gSonar db.

        User need to provide gSonarTableName, which is the table's base name.
    """

    @abc.abstractmethod
    def gSonarTableName(self):
        """Table's base name. The schema name is provided from runtime config 'hivedb'"""

    def tableName(self):
        return self.smvGetRunConfig('hivedb') + "." + self.gSonarTableName()


class HiveWithSampling(GSonarHiveTable, SmvRunConfig):
    """Input tables which need to be sampled at run-time should be
    derived from this ABC
    """

    @abc.abstractmethod
    def sampling_key(self):
        """Sampling key's column name as a string"""

    def isEphemeral(self):
        """Ephemeral if no sampling, otherwise not ephemeral. So that
        sampling result can be persisted
        """
        if (self.smvGetRunConfig("sample") == "full"):
            return True
        else:
            return False

    def run(self, df):
        return do_sample(df, self.sampling_key(), self.smvGetRunConfig("sample"))

claimUtils = ClaimUtils({
    "ptnt_id": "ptnt_gid",
    "phyn_id": "phyn_gid",
    "clm_id":  "clm_gid",
    "dt": "dt"
})

hier_utils = HierarchyUtils(
    'geo',
    ('hsa', ['zip', 'hsa', 'hrr', 'country']),
    ('county', ['zip', 'county', 'state', 'country'])
)

hier_utils_hsa = HierarchyUtils(
    'geo',
    ('hsa', ['hsanum', 'hsa', 'hrr', 'country'])
)

hier_utils_hrr = HierarchyUtils(
    'geo',
    ('hrr', ['hrrnum', 'hrr', 'country'])
)

hier_keys = ['geo_type', 'geo_value', 'geo_name']

hier_keys_with_parent = [
    'geo_type', 'geo_value', 'geo_name',
    'parent_geo_type', 'parent_geo_value', 'parent_geo_name'
    ]

def applyMap(hashmap, col, default=''):
    """returns the map's value for the given key if present, and the default otherwise.

        Can't use udf since Spark 1.5.0 python udf has issues. Implemented as when().otherwise()
    """
    keys = hashmap.keys()
    to_be_reduced = [when(col == keys[0], lit(hashmap.get(keys[0])))] + keys[1:]
    when_chain = reduce(lambda cond, c: cond.when(col == c, lit(hashmap[c])), to_be_reduced)
    return when_chain.otherwise(lit(default))


def scaleRate(rateCol, popCol):
    return (sum(rateCol * popCol) / sum('pop'))
