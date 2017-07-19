from smv import *
from pyspark.sql.functions import *
from twinelib.utils import ClaimUtils
from magbc.core import targetDrugList
import inputdata, lot


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


class PhysicianLevelStats(SmvModule, SmvOutput):
    """
    Aggregate target drug stats to physician level
    """
    def requiresDS(self):
        return [InscopeClaims, PrimPhyn, lot.Lot]

    def run(self, i):
        clm = i[InscopeClaims]
        pcp = i[PrimPhyn]
        therpy = i[lot.Lot]

        lots = therpy.groupBy("ptnt_gid"
            ).agg(max("LOT").alias("maxLOT")
            ).smvJoinByKey(pcp, ["ptnt_gid"], "leftOuter"
            ).groupBy("prim_phyn_gid"
            ).agg(avg("maxLOT").alias("Average_Line_Of_Therapy"))

        stats = clm.smvJoinByKey(pcp, ["ptnt_gid"], "leftOuter"
            ).filter(col("drugType") == "TARGET"
            ).groupBy("prim_phyn_gid"
            ).pivot("prodt_cd"
            ).agg(sum(lit(1))
            )

        return stats.smvRenameField(*[(x, "NumberOfPrescriptions_" + x) for x in targetDrugList]
            ).smvJoinByKey(lots, ["prim_phyn_gid"], "leftouter")

