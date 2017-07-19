from smv import *
from smv.error import SmvRuntimeError

from pyspark.sql.functions import *
from smv.functions import *
from smv.matcher import *
import re

def addressId(zip5, addr):
    def id(s1, s2):
        if (s1 is None or s2 is None):
            return None
        else:
            m = re.search('(\d+)', s2)
            if (m is not None):
                return s1 + m.group()
            else:
                return None

    g = udf(id)
    return g(zip5, addr)


def fullnamescore(fns, lns):
    def score (sc1, sc2): 
        if (sc1 is None or sc2 is None):
            return None
        else:
            return (sc1 + sc2)/2.0

    sf = udf(score)
    return sf(fns, lns)

class MatcherUtils(object):
    """Util methods for SmvEntityMatcher

        Args:
            _colMap (dictionary): data column name mapping for 2 sources

        Example:
            code::

                claimUtils = ClaimUtils({
                    "id": ["Physician_Profile_ID", \
                           "NPI"],
                    "first_name": ["Physician_First_Name", \
                                   "Provider_First_Name"],
                    "last_name": ["Physician_Last_Name", \
                                  "Provider_Last_Name_Legal_Name"],
                    "address_first_line": ["Recipient_Primary_Business_Street_Address_Line1", \
                                           "Provider_First_Line_Business_Practice_Location_Address"],
                    "zip": ["Recipient_Zip_Code", \
                            "Provider_Business_Practice_Location_Address_Postal_Code"]
                })
    """
    def __init__(self, _colMap):
        self.colMap = _colMap

    def _check_cols(self, df, colMap, *cols):
        for c in cols:
            if not (c in colMap):
                raise SmvRuntimeError("Column key pair {0} is not defined in the colMap.".format(c))
            else:
                colName = colMap.get(c)
                if not (colName in df.columns):
                    raise SmvRuntimeError("Column {0} does not exist in the given DataFrame".format(colName))

    def physicianMatcher(self, df1, df2):
        """match entity from 2 sources

           Args:
               df1 (DataFrame): entity source 1 descriptives
               df2 (DataFrame): entity source 2 descriptives

           Returns:
               (DataFrame): The result data frame returns SmvEntityMatcher result

           Expected input:
             * id 
             * first_name
             * last_name 
             * address_first_line
             * zip

        """
        c1 = map(dict, zip(*[[(k, v) for v in value] for k, value in self.colMap.items()]))[0]
        c2 = map(dict, zip(*[[(k, v) for v in value] for k, value in self.colMap.items()]))[1]

        self._check_cols(df1, c1, "id", "first_name", "last_name", "address_first_line", "zip")
        self._check_cols(df2, c2, "id", "first_name", "last_name", "address_first_line", "zip")

        def dfNorm(df, c):
            return df.filter(
                length(col(c["zip"])) >= 5
                ).filter(
                col(c["id"]).isNotNull()
                ).select(
                col(c["id"]).alias("id"),
                upper(col(c["first_name"])).alias("fstn"),
                upper(col(c["last_name"])).alias("lstn"),
                col(c["zip"]).substr(1,5).alias("zip5"),
                col(c["address_first_line"]).alias("adrsl1")
                ).withColumn("fuln",
                smvStrCat(col("lstn"), col("fstn"))
                ).withColumn("adrsId",
                addressId(col("zip5"), col("adrsl1"))
                )

        ndf1 = dfNorm(df1, c1)
        ndf2 = dfNorm(df2, c2).smvPrefixFieldNames('_')

        result = SmvEntityMatcher('id', '_id',
        ExactMatchPreFilter(
            "AddressId_FullName_Match", 
            concat(col("adrsId"), col("fuln")) == \
            concat(col("_adrsId"),col("_fuln"))
            ),
        GroupCondition(
            soundex(col("lstn")) == soundex(col("_lstn"))
            ),
        [
          FuzzyLogic("AddressId_FuzzyFullName_Match",
            col("adrsId") == col("_adrsId"),
            fullnamescore(
            jaroWinkler(col("lstn"), col("_lstn")), 
            jaroWinkler(col("fstn"), col("_fstn"))), 
            0.9),
          ExactLogic("FullName_Zip_Match", 
            concat(col("fuln"), col("zip5")) == \
            concat(col("_fuln"), col("_zip5")))
        ]
        ).doMatch(ndf1, ndf2, False)

        return result





