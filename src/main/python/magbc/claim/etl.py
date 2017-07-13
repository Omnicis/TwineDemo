from smv import *
from pyspark.sql.functions import *
from twinelib.utils import ClaimUtils
from magbc.core import targetDrugList
import inputdata


class InscopeClaims(SmvModule, SmvOutput):
    """
    Filter by target products &
    Append product information 
    """
    def requiresDS(self):
        return [inputdata.Claims, 
        inputdata.ProductMaster]

    def run(self, i):
        clm = i[inputdata.Claims]
        pdt = i[inputdata.ProductMaster]

        clmWithInfo = clm.smvJoinByKey(pdt, ["prodt"], "inner")

        return clmWithInfo


class PrimPhyn(SmvModule):
    """
    For each patient identify the primary physician &
    Append primary physician's zip
    """
    def requiresDS(self):
        return [InscopeClaims]

    def run(self, i):
        df = i[InscopeClaims]

        claimUtils = ClaimUtils({
                    "ptnt_id": "ptnt_gid",
                    "phyn_id": "phyn_gid",
                    "clm_id":  "clm_gid",
                    "dt": "dt"})

        primPh = claimUtils.toPtntPrimPhyn(df)
        return primPh


class PhysicianLevelStats(SmvModule):
    """
    Aggregate target drug stats to physician level
    """
    def requiresDS(self):
        return [InscopeClaims, PrimPhyn]

    def run(self, i):
        clm = i[InscopeClaims]
        pcp = i[PrimPhyn]

        stats = clm.smvJoinByKey(pcp, ["ptnt_gid"], "leftOuter"
            ).filter(col("drugType") == "TARGET"
            ).groupBy("prim_phyn_gid", "prim_phyn_zip_cde"
            ).pivot("prodt_cd"
            ).agg(sum(lit(1))
            )

        return stats.smvRenameField(*[(x, "NumberOfPrescriptions_" + x) for x in targetDrugList])

