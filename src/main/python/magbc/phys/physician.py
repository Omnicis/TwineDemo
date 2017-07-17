from smv import *
from pyspark.sql.functions import *
import inputdata


class AllPhynStats(SmvModule, SmvOutput):
    """
    Join physician level stats from public domain and claim
    """
    def requiresDS(self):
        return [inputdata.PhysicianLevelStats,
        inputdata.OpenPaymentPhynStats,
        inputdata.PhysicianMatch,
        inputdata.PhynProfile]

    def run(self, i):
        clm = i[inputdata.PhysicianLevelStats]
        op = i[inputdata.OpenPaymentPhynStats]
        pp = i[inputdata.PhynProfile]
        pm = i[inputdata.PhysicianMatch]

        opk = pm.select("Physician_Profile_ID", "NPI"
            ).smvDedupByKey("Physician_Profile_ID", "NPI"
            ).smvJoinByKey(op, ["Physician_Profile_ID"], "inner")

        ppk = pp.select(
            col("phyn_gid").alias("prim_phyn_gid"), col("mdm_npi").alias("NPI"),
            col("phyn_fst_nm"), col("phyn_mid_nm"), col("phyn_last_nm"), col("phyn_gender"),
            col("phyn_addr"), col("phyn_city"), col("phyn_st"), col("phyn_zip_cde"),
            col("mdm_prim_specialty_code")
            )

        return clm.smvJoinByKey(ppk, ["prim_phyn_gid"], "leftouter"
            ).smvJoinByKey(opk, ["NPI"], "leftouter")