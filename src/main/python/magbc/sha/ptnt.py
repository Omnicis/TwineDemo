from smv import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import inputdata
from twinelib.utils import ClaimUtils

class LotDF(SmvModule):
    def requiresDS(self):
        return[inputdata.LoT]
    def run(self, i):
        df = i[inputdata.LoT]
        return df

class InscopePt(SmvModule):
    """
    filter claim with pt with at least 3 claims
    """
    def requiresDS(self):
        return[inputdata.LoT, inputdata.FilteredClaims]

    def run(self, i):
#        lot = i[inputdata.LoT]
        claim = i[inputdata.FilteredClaims]

#        line = lot.smvJoinByKey(claim, ["ptnt_gid"], "leftouter"
#            ).groupBy("ptnt_gid").count().groupBy().agg(min("count").alias("MinLine")
#            ).collect()[0].asDict()["MinLine"]
#        return claim.groupBy("ptnt_gid").count(
#            ).filter(col("count") >= line
#            ).smvDedupByKey("ptnt_gid"
#            ).select("ptnt_gid")

        ptnt_claim_cnt = claim.select('ptnt_gid', 'clm_gid').distinct()\
            .groupBy('ptnt_gid').agg(count('clm_gid').alias('N'))

        return ptnt_claim_cnt.where(col('N') >= 3).select('ptnt_gid')


class TargetClaims(SmvModule):
    """
    Filter AllClaims with All relevant treatment & Diag
    """
    def requiresDS(self):
        return [inputdata.FilteredClaims, InscopePt]

    def run(self, i):
        df = i[inputdata.FilteredClaims]
        scope = i[InscopePt]
        dfInscope = df.smvJoinByKey(scope, ["ptnt_gid"], "inner")

        Flag = dfInscope.withColumn("IsBreastCancer",
            when(col("diagArr").smvIsAnyIn(*BreastCancer), True)
            ).withColumn("IsTreament",
            when((col("targetType") == "Drug")|(col("procType") == "Surgery"), True)
            #Testing flags
            ).withColumn("IsHer2Test",
            when((col("procSubType") == "IHC")|(col("procSubType") == "ISH"), True)
            ).withColumn("IsIHCTest",
            when(col("procSubType") == "IHC", True)
            ).withColumn("IsISHTest",
            when(col("procSubType") == "ISH", True)
            ).withColumn("IsBRCATest",
            when(col("procSubType") == "BRCA", True)
            #Surgery flags
            ).withColumn("IsSurgery",
            when(col("procType") == "Surgery", True)
            ).withColumn("IsPartialSurgery",
            when(col("procSubType") == "Partial", True)
            ).withColumn("IsSubcutaneousSurgery",
            when(col("procSubType") == "Subcutaneous", True)
            ).withColumn("IsSimpleTotalSurgery",
            when(col("procSubType") == "Simple Total", True)
            ).withColumn("IsModifiedRadicalSurgery",
            when(col("procSubType") == "Modified Radical", True)
            ).withColumn("IsRadicalSurgery",
            when(col("procSubType") == "Radical", True)
            #HER2 target therapy flags
            ).withColumn("IsHer2Target",
            when(col("drugType") == "TARGET", True)
            ).withColumn("IsTrastuzumab",
            when(col("prodt_cd") == "HERCEPTIN", True)
            ).withColumn("IsPertuzumab",
            when(col("prodt_cd") == "PERJETA", True)
            ).withColumn("IsLapatinib",
            when(col("prodt_cd") == "TYKERB", True)
            ).withColumn("IsTDM1",
            when(col("prodt_cd") == "KADCYLA", True)
            #Chemo flags
            ).withColumn("IsChemo",
            when(col("drugType") == "CHEMO", True)
            #HORMONE flags
            ).withColumn("IsHormone",
            when(col("drugType") == "HORMONE", True))
        return Flag


class TargetDates(SmvModule):
    """
    pt level, extract first and last dates from TargetClaims
    """
    def requiresDS(self):
        return [TargetClaims]

    def run(self, i):
        claim = i[TargetClaims]

        date = claim.select(
            col("ptnt_gid"),
            col("dt"),
            when(col("IsBreastCancer"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("BreastDiagDate"),
            when(col("IsHer2Test"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("Her2TestDate"),
            when(col("IsIHCTest"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("IHCTestDate"),
            when(col("IsISHTest"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("ISHTestDate"),
            when(col("IsBRCATest"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("BRCATestDate"),
            when(col("IsTreament"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("TreamentDate"),
            when(col("IsHer2Target"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("Her2TargetDate"),
            when(col("IsChemo"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("ChemoDate"),
            when(col("IsHormone"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("HormoneDate"),
            when(col("IsSurgery"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("SurgeryDate"),
            when(col("IsTrastuzumab"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("TrastuzumabDate"),
            when(col("IsPertuzumab"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("PertuzumabDate"),
            when(col("IsLapatinib"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("LapatinibDate"),
            when(col("IsTDM1"), col("dt")).otherwise(lit(None).cast(TimestampType())).alias("TDM1Date")
            ).orderBy(col("dt").asc()
            ).groupBy("ptnt_gid"
            ).agg(
            first(col("BreastDiagDate")).alias("First_BreastDiagDate"),
            first(col("Her2TestDate")).alias("First_Her2TestDate"),
            first(col("IHCTestDate")).alias("First_IHCTestDate"),
            first(col("ISHTestDate")).alias("First_ISHTestDate"),
            first(col("BRCATestDate")).alias("First_BRCATestDate"),
            first(col("TreamentDate")).alias("First_TreamentDate"),
            first(col("Her2TargetDate")).alias("First_Her2TargetDate"),
            first(col("ChemoDate")).alias("First_ChemoDate"),
            first(col("HormoneDate")).alias("First_HormoneDate"),
            first(col("SurgeryDate")).alias("First_SurgeryDate"),
            first(col("TrastuzumabDate")).alias("First_TrastuzumabDate"),
            first(col("PertuzumabDate")).alias("First_PertuzumabDate"),
            first(col("LapatinibDate")).alias("First_LapatinibDate"),
            first(col("TDM1Date")).alias("First_TDM1Date"),
            last(col("TreamentDate")).alias("Last_TreamentDate"),
            last(col("Her2TargetDate")).alias("Last_Her2TargetDate"),
            last(col("ChemoDate")).alias("Last_ChemoDate"),
            last(col("HormoneDate")).alias("Last_HormoneDate"),
            last(col("SurgeryDate")).alias("Last_SurgeryDate"),
            last(col("TrastuzumabDate")).alias("Last_TrastuzumabDate"),
            last(col("PertuzumabDate")).alias("Last_PertuzumabDate"),
            last(col("LapatinibDate")).alias("Last_LapatinibDate"),
            last(col("TDM1Date")).alias("Last_TDM1Date")
            )
        return date


class TestStatsPtLevel(SmvModule):
    """
    Get Her2 test stats at pt level
    """
    def requiresDS(self):
        return[TargetDates]

    def run(self, i):
        dates = i[TargetDates]

        pts = dates.filter(col("First_BreastDiagDate").isNotNull())

        ptsT = pts.withColumn("DiagToTestDays", datediff(col("First_Her2TestDate"), col("First_BreastDiagDate"))
            ).select(
            col("ptnt_gid"),
            col("DiagToTestDays"),
            when(col("First_Her2TestDate").isNotNull(), lit(1)).otherwise(lit(0)).alias("Her2Tested"),
            when(col("First_IHCTestDate").isNotNull(), lit(1)).otherwise(lit(0)).alias("IHCTested"),
            when(col("First_ISHTestDate").isNotNull(), lit(1)).otherwise(lit(0)).alias("ISHTested"),
            when(col("First_BRCATestDate").isNotNull(), lit(1)).otherwise(lit(0)).alias("BRCATested"),
            when(col("First_Her2TargetDate").isNotNull(), lit(1)).otherwise(lit(0)).alias("Her2Treated")
            )

        return ptsT


class PtntPrimPhyn(SmvModule):
    """
    For each ptnt identify the primary physician
    """
    def requiresDS(self):
        return [TargetClaims, inputdata.AllPhyns]

    def run(self, i):
        df = i[TargetClaims]
        ph = i[inputdata.AllPhyns]

        ph2 = ph.smvRenameField(*[(x, "prim_" + x) for x in ph.columns])

        claimUtils = ClaimUtils({
                    "ptnt_id": "ptnt_gid",
                    "phyn_id": "phyn_gid",
                    "clm_id":  "clm_gid",
                    "dt": "dt"})

        primPh = claimUtils.toPtntPrimPhyn(df).smvJoinByKey(ph2, ['prim_phyn_gid'], 'leftouter')
        return primPh


class TestStatsPtLevelWithZip(SmvModule):
    """TestStatsPtLevel with primary physician's zip code as ptnt zip for aggregations
    """

    def requiresDS(self):
        return [TestStatsPtLevel, PtntPrimPhyn]

    def run(self, i):
        test = i[TestStatsPtLevel]
        pt_phy = i[PtntPrimPhyn].select(
            "ptnt_gid",
            col("prim_phyn_zip_cde").substr(1, 5).alias('zip')
        )

        return test.smvJoinByKey(pt_phy, ['ptnt_gid'], 'inner')


#class HER2PtDriver(SmvModule):
#    """
#    Get Her2 patient pool
#    """
#    #TODO: need to break into MetastaticPtntDirver VS Non-MetaPtntDriver
#    #####ALARM: pool is very small due to her2 treatment rate very low, need to figure out why!
#    def requiresDS(self):
#        return [TargetDates]
#
#    def run(self, i):
#        dates = i[TargetDates]
#        ## criteria: newly diagnosed
#        diagNew = dates.select(col("ptnt_gid"),
#            col("First_BreastDiagDate").smvYear().alias("BreastDiagYear")
#            # this cut-off may need to be adjusted
#            ).filter(col("BreastDiagYear") >= 2013)
#        ## criteria: HER2+
#        treatHer2 = dates.filter(col("First_Her2TargetDate").isNotNull())
#
#        return diagNew.smvJoinByKey(treatHer2, ["ptnt_gid"], "inner"
#            ).select("ptnt_gid")


# TODO: need to put this into refinput.py
BreastCancer = ["174",
                "174.0", "C50.011", "C50.012", "C50.019",
                "174.1", "C50.111", "C50.112", "C50.119",
                "174.2", "C50.211", "C50.212", "C50.219",
                "174.3", "C50.311", "C50.312", "C50.319",
                "174.4", "C50.411", "C50.412", "C50.419",
                "174.5", "C50.511", "C50.512", "C50.519",
                "174.6", "C50.611", "C50.612", "C50.619",
                "174.8", "C50.819",
                "174.9", "C50.911", "C50.912", "C50.919",
                "175.0", "C50.021", "C50.022", "C50.029",
                "175.9", "C50.921", "C50.922", "C50.929",
                "233.0", "D05.90", "D05.91", "D05.92",
                "239.3", "D49.3",
                "C50.811", "C50.812"]
