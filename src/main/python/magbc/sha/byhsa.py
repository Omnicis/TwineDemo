from smv import *
from smv.functions import *

from pyspark.sql.functions import lit, col, count, sum, avg, when, sort_array, first, min, max, percent_rank, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

import inputdata
import ptnt
from magbc.core import applyMap, hier_utils, hier_keys
from twinelib.hier import HierarchyUtils

def safe_div(cn1, cn2):
    return when(col(cn2) > 0, (col(cn1) / col(cn2)).cast('float'))\
        .when((col(cn2).isNull() | (col(cn2) == 0)), 0.0)\
        .otherwise(lit(None).cast('float'))

def avg_rm_zero(cname):
    return avg(when(col(cname) == 0, lit(None).cast('float')).otherwise(col(cname).cast('float')))


class ZipHierMap(SmvModule, SmvOutput):
    """Standardize ZipMaster for hierarchy aggregation
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

class LotRgmnReGroup(SmvModule, SmvRunConfig):
    """Lot with regrouped Cohort
    """

    def requiresDS(self):
        return [inputdata.LoT]

    def run(self, i):
        lot = i[inputdata.LoT]

        startyear = int(self.smvGetRunConfig('startyear'))
        endyear = int(self.smvGetRunConfig('endyear'))

        lot_rgrp = lot\
            .where(lot.lot_frst_dt.smvYear() <= endyear)\
            .where(lot.lot_lst_dt.smvYear() >= startyear)\
            .withColumn('zip', lot.phyn_zip_cd_lot.substr(0, 5))\
            .withColumn(
                'cohort',
                when(lot.lot == 0.1, lit('neoadjuvant')).
                when(lot.lot == 0.2, lit('adjuvant')).
                when(lot.lot == 1, lit('1L')).
                when(lot.lot > 1, lit('2Lplus')).
                otherwise(lit(None).cast("string"))
            ).withColumn(
                'phase',
                when(lot.lot == 0.1, lit('neoadjuvant')).
                when(lot.lot == 0.2, lit('adjuvant')).
                when(lot.lot == 1, lit('1L')).
                when(lot.lot == 2, lit('2L')).
                otherwise(lit(None).cast("string"))
            )

        return lot_rgrp


class PhyByHsa(SmvModule, SmvOutput):
    """geo level primary physician count
    """

    def requiresDS(self):
        return [ZipHierMap, LotRgmnReGroup]

    def run(self, i):
        lot = i[LotRgmnReGroup]
        zipmap = i[ZipHierMap]

        def genderPercent(col):
            return (sum(when(col == 'M', lit(1)).otherwise(lit(0))) /
                    sum(when(col.isin('M','F'), lit(1)).otherwise(lit(0))))

        lotphy = lot.select(
            "prmry_phy_lot",
            "phyn_zip_cd_lot",
            "phyn_gender_lot",
            ).smvDedupByKey("prmry_phy_lot"
            ).withColumn('zip', col("phyn_zip_cd_lot").substr(0, 5)
            )

        lotphy_geo = hier_utils.hier_agg(
            lotphy,
            zipmap,
            'hsa', 'hrr', 'country'
            )(sum(lit(1)).alias("total_prmry_phy"),
            genderPercent(col("phyn_gender_lot")).alias("pct_phyn_gender_male"))

        return lotphy_geo


class LotByCohortHsaOriginalRgmn(SmvModule):
    """Hsa ptnt count with original Rgmn pivoted
    """

    def requiresDS(self):
        return [ZipHierMap, LotRgmnReGroup]

    def run(self, i):
        lot = i[LotRgmnReGroup]
        zipmap = i[ZipHierMap]

        rgmn = [
            "HER_mono",
            "HER_AC",
            "HER_FEC",
            "HER_KAD",
            "HER_OTHER",
            "HER_PER_OTHER",
            "HER_PER_KAD",
            "HER_PER_TAXANE",
            "HER_TAXANE",
            "KAD",
            "KAD_OTHER",
            "KAD_PER",
            "OTHER",
            "PER_OTHER",
            "PER_TAXANE",
            "TCH"
        ]

        lot_pivoted = lot.withColumn("ptnt_cnt", lit(1))\
            .smvGroupBy('ptnt_gid', 'lot', 'zip', 'cohort')\
            .smvPivot([['new_lot_rgmn']], ['ptnt_cnt'], rgmn)

        return hier_utils.hier_agg(
            lot_pivoted.smvGroupBy('cohort'),
            zipmap,
            'hsa', 'hrr', 'country'
        )(*([sum(lit(1)).alias('ptnt_cnt_tot')] +
            [sum(col('ptnt_cnt_' + r)).alias('ptnt_cnt_' + r) for r in rgmn]
        )).select(
            ['cohort'] + hier_keys + ['ptnt_cnt_tot'] +
            [safe_div('ptnt_cnt_' + r, 'ptnt_cnt_tot').alias('ptnt_pct_' + r) for r in rgmn]
        )



class LotByCohortHsaGeneral(SmvModule):
    """
    ExamplePyModule Description
    """

    def requiresDS(self):
        return [ZipHierMap, LotRgmnReGroup]

    def run(self, i):
        lot = i[LotRgmnReGroup]
        zipmap = i[ZipHierMap].where(col('hsa').isNotNull())

        def cnt_true(c):
            return sum(when(c, lit(1)).otherwise(lit(0)))

        agged = hier_utils.hier_agg(
            lot.smvGroupBy('cohort'),
            zipmap,
            'hsa', 'hrr', 'country'
        )(
            count(lit(1)).alias('ptnt_cnt'),
            cnt_true(lot.tot_her_inf > 0).alias('ptnt_use_her'),
            avg_rm_zero('tot_her_inf').alias('avg_her_inf'),
            avg_rm_zero('her_cmplnt_drtn').alias('avg_her_cmplnt_drtn'),
            cnt_true(lot.tot_per_inf > 0).alias('ptnt_use_per'),
            avg_rm_zero('tot_per_inf').alias('avg_per_inf'),
            avg_rm_zero('per_cmplnt_drtn').alias('avg_per_cmplnt_drtn'),
            cnt_true(lot.tot_kad_inf > 0).alias('ptnt_use_kad'),
            avg_rm_zero('tot_kad_inf').alias('avg_kad_inf'),
            avg_rm_zero('kad_cmplnt_drtn').alias('avg_kad_cmplnt_drtn'),
            cnt_true(lot.tot_tax_inf > 0).alias('ptnt_use_tax'),
            avg_rm_zero('tot_tax_inf').alias('avg_tax_inf'),
            cnt_true(lot.tot_rmvl_surg.cast('float') > 0).alias('ptnt_surg_rmvl'),
            cnt_true(lot.tot_recons_surg.cast('float') > 0).alias('ptnt_surg_recons'),
            sum(col('hom_usage_inline')).alias('ptnt_hom_usage_inline'),
            # ptnt_denovo_1l all nulls
            # sum(col('ptnt_denovo_1l_flag')).alias('ptnt_denovo_1l'),
            sum(col('k_ex')).alias('ptnt_1l_kad')
        )\
            .withColumn('ptnt_pct_use_her', safe_div('ptnt_use_her', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_per', safe_div('ptnt_use_per', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_kad', safe_div('ptnt_use_kad', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_tax', safe_div('ptnt_use_tax', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_surg_rmvl', safe_div('ptnt_surg_rmvl', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_surg_recons', safe_div('ptnt_surg_recons', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_hom', safe_div('ptnt_hom_usage_inline', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_kad_1l', safe_div('ptnt_1l_kad', 'ptnt_cnt'))

        return agged


class LotByPhaseHsaGeneral(SmvModule):
    """
    ExamplePyModule Description
    """

    def requiresDS(self):
        return [ZipHierMap, LotRgmnReGroup]

    def run(self, i):
        lot = i[LotRgmnReGroup].filter(col("phase").isNotNull())
        zipmap = i[ZipHierMap].where(col('hsa').isNotNull())

        def cnt_true(c):
            return sum(when(c, lit(1)).otherwise(lit(0)))

        agged = hier_utils.hier_agg(
            lot.smvGroupBy('phase'),
            zipmap,
            'hsa', 'hrr', 'country'
        )(
            count(lit(1)).alias('ptnt_cnt'),
            cnt_true(lot.tot_her_inf > 0).alias('ptnt_use_her'),
            avg_rm_zero('tot_her_inf').alias('avg_her_inf'),
            avg_rm_zero('her_cmplnt_drtn').alias('avg_her_cmplnt_drtn'),
            cnt_true(lot.tot_per_inf > 0).alias('ptnt_use_per'),
            avg_rm_zero('tot_per_inf').alias('avg_per_inf'),
            avg_rm_zero('per_cmplnt_drtn').alias('avg_per_cmplnt_drtn'),
            cnt_true(lot.tot_kad_inf > 0).alias('ptnt_use_kad'),
            avg_rm_zero('tot_kad_inf').alias('avg_kad_inf'),
            avg_rm_zero('kad_cmplnt_drtn').alias('avg_kad_cmplnt_drtn'),
            cnt_true(lot.tot_tax_inf > 0).alias('ptnt_use_tax'),
            avg_rm_zero('tot_tax_inf').alias('avg_tax_inf'),
            cnt_true(lot.tot_rmvl_surg.cast('float') > 0).alias('ptnt_surg_rmvl'),
            cnt_true(lot.tot_recons_surg.cast('float') > 0).alias('ptnt_surg_recons'),
            sum(col('hom_usage_inline')).alias('ptnt_hom_usage_inline'),
            # ptnt_denovo_1l all nulls
            # sum(col('ptnt_denovo_1l_flag')).alias('ptnt_denovo_1l'),
            sum(col('k_ex')).alias('ptnt_1l_kad')
        )\
            .withColumn('ptnt_pct_use_her', safe_div('ptnt_use_her', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_per', safe_div('ptnt_use_per', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_kad', safe_div('ptnt_use_kad', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_tax', safe_div('ptnt_use_tax', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_surg_rmvl', safe_div('ptnt_surg_rmvl', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_surg_recons', safe_div('ptnt_surg_recons', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_hom', safe_div('ptnt_hom_usage_inline', 'ptnt_cnt'))\
            .withColumn('ptnt_pct_use_kad_1l', safe_div('ptnt_1l_kad', 'ptnt_cnt'))

        return agged


class GeoStdOfCare(SmvModule, SmvOutput):
    """
    data for heatmap at hrr hsa level with 4 targets
    """

    def requiresDS(self):
        return [LotByPhaseHsaGeneral]

    def run(self, i):
        phase = i[LotByPhaseHsaGeneral]

        ptnt_cnt = phase.groupBy(*hier_keys).agg(sum('ptnt_cnt').alias('ptnt_cnt'))
        phasePivot = phase.smvGroupBy("geo_type", "geo_value", "geo_name"
            ).smvPivotCoalesce(
            [['phase']],
            ['ptnt_pct_use_per',
            'avg_her_cmplnt_drtn',
            'ptnt_pct_use_kad'],
            ["neoadjuvant", "adjuvant", "1L", "2L"]
            ).select(
            "geo_type",
            "geo_value",
            "geo_name",
            "ptnt_pct_use_per_neoadjuvant",
            "avg_her_cmplnt_drtn_adjuvant",
            "ptnt_pct_use_per_1L",
            "ptnt_pct_use_kad_2L")

        return ptnt_cnt.smvJoinByKey(phasePivot, hier_keys, 'inner')


class LotByCohortHsa(SmvModule, SmvOutput):
    """Combined LoT on HSA hierarchy summary
    """

    def requiresDS(self):
        return [LotByCohortHsaOriginalRgmn, LotByCohortHsaGeneral]

    def run(self, i):
        df_gen = i[LotByCohortHsaGeneral]
        df_rgm = i[LotByCohortHsaOriginalRgmn]

        res = df_gen.smvJoinByKey(
            df_rgm.drop('ptnt_cnt_tot'),
            ['cohort'] + hier_keys,
            'inner'
        )

        return res


class LotPtntJourneyCnt(SmvModule):
    """Number of ptnt who has neoadjuvant only, neo+adj, etc. by HSA

        Histogram of cn_comp: String sort by Key
        key                      count      Pct    cumCount   cumPct
        1                         1643    1.47%        1643    1.47%
        1_2                       8978    8.01%       10621    9.47%
        1_2_3                      698    0.62%       11319   10.09%
        1_2_3_4                    363    0.32%       11682   10.42%
        1_3                       1288    1.15%       12970   11.57%
        1_3_4                      397    0.35%       13367   11.92%
        1_4                         69    0.06%       13436   11.98%
        2                        63616   56.73%       77052   68.71%
        2_3                      11014    9.82%       88066   78.53%
        2_3_4                     7082    6.32%       95148   84.85%
        3                        10485    9.35%      105633   94.20%
        3_4                       6509    5.80%      112142  100.00%
    """

    def requiresDS(self):
        return [LotRgmnReGroup]

    def run(self, i):
        lot = i[LotRgmnReGroup]

        cohort_map = {
            'neoadjuvant': "1",
            'adjuvant': "2",
            '1L': "3",
            "2Lplus": "4"
        }

        ptnt_lvl = lot.withColumn('cn', applyMap(cohort_map, lot.cohort, ""))\
            .groupBy('ptnt_gid').agg(
                first(lot.zip).alias('zip'),
                smvCollectSet(col('cn'), StringType()).alias('cn_set')
            ).select(
                'ptnt_gid',
                'zip',
                smvArrayCat("_", sort_array(col('cn_set'))).alias('cn_comp')
            )

        return ptnt_lvl


class TestStatsGeoLevel(SmvModule, SmvOutput):
    """
    Get Her2 test stats at Geo Level
    """
    def requiresDS(self):
        return[ptnt.TestStatsPtLevelWithZip, ZipHierMap]

    def run(self, i):
        zipmap = i[ZipHierMap]
        test = i[ptnt.TestStatsPtLevelWithZip]

        # Assume that all Her2 treated ptnts should all have Her2 tests
        # In case that some ptnts missing the test claims, we consider
        # them caused by data capture issue.
        # We assume this data capture issue is evenly distributed in the
        # entire country, or there are not discrepancy by geo on capture rate
        capture_rate = test.filter(col("Her2Treated") == 1).agg(
                sum("Her2Treated").alias("Her2Count"),
                sum("Her2Tested").alias("TestCount")
            ).select(
                (col("TestCount")/col("Her2Count")).alias("CaptureRate")
            ).collect()[0][0]

        def cap_to_one(c):
            return when(c > 1.0, 1.0).otherwise(c)

        geo_agged = hier_utils.hier_agg(
            test,
            zipmap,
            'hsa', 'hrr', 'country'
        )(
            count("ptnt_gid").alias("breast_ptnt_cnt"),
            sum("Her2Tested").alias("Her2TestCount"),
            sum("IHCTested").alias("IHCTestCount"),
            sum("ISHTested").alias("ISHTestCount"),
            sum("BRCATested").alias("BRCATestCount")
        )\
        .withColumn("Her2TestRate", safe_div("Her2TestCount", "breast_ptnt_cnt"))\
        .withColumn("IHCTestRate", safe_div("IHCTestCount", "breast_ptnt_cnt"))\
        .withColumn("ISHTestRate", safe_div("ISHTestCount", "breast_ptnt_cnt"))\
        .withColumn("BRCATestRate", safe_div("BRCATestCount", "breast_ptnt_cnt"))\
        .withColumn("Her2TestRate_Scaled", cap_to_one(col("Her2TestRate")/capture_rate))\
        .withColumn("IHCTestRate_Scaled", cap_to_one(col("IHCTestRate")/capture_rate))\
        .withColumn("ISHTestRate_Scaled", cap_to_one(col("ISHTestRate")/capture_rate))\
        .withColumn("BRCATestRate_Scaled", cap_to_one(col("BRCATestRate")/capture_rate))

        return geo_agged
