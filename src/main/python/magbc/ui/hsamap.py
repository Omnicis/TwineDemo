from smv import *
import inputdata
from magbc.core import hier_utils, hier_keys, hier_keys_with_parent, applyMap
from pyspark.sql.functions import lit, col, min, max, sum, avg, sqrt, percent_rank, rank, desc, regexp_replace
from pyspark.sql.window import Window

import account

#ZipHierMap = SmvModuleLink(byhsa.ZipHierMap)
#LotByCohortHsa = SmvModuleLink(byhsa.LotByCohortHsa)
#TestStatsGeoLevel = SmvModuleLink(byhsa.TestStatsGeoLevel)

by_cohort_vars = [
    "ptnt_pct_use_her",
    "avg_her_inf",
    "avg_her_cmplnt_drtn",

    "ptnt_pct_use_per",
    "avg_per_inf",
    "avg_per_cmplnt_drtn",

    "ptnt_pct_use_kad",
    "avg_kad_inf",
    "avg_kad_cmplnt_drtn",

    "ptnt_pct_use_tax",
    "avg_tax_inf",

    "ptnt_pct_surg_rmvl",
    "ptnt_pct_surg_recons",
    "ptnt_pct_use_hom",
    "ptnt_pct_use_kad_1l",

    "ptnt_pct_HER_mono",
    "ptnt_pct_HER_AC",
    "ptnt_pct_HER_FEC",
    "ptnt_pct_HER_KAD",
    "ptnt_pct_HER_OTHER",
    "ptnt_pct_HER_PER_OTHER",
    "ptnt_pct_HER_PER_KAD",
    "ptnt_pct_HER_PER_TAXANE",
    "ptnt_pct_HER_TAXANE",
    "ptnt_pct_KAD",
    "ptnt_pct_KAD_OTHER",
    "ptnt_pct_KAD_PER",
    "ptnt_pct_OTHER",
    "ptnt_pct_PER_OTHER",
    "ptnt_pct_PER_TAXANE",
    "ptnt_pct_TCH"
]

cohorts = ['neoadjuvant', 'adjuvant', '1L', '2Lplus']


def str_list_dot(s1, s2):
    return [c1 + "_" + c2 for c1 in s1 for c2 in s2]

def flatten(l):
    return [item for sublist in l for item in sublist]

class LotByHsaPivotCohort(SmvModule):
    """LotByCohortHsa with Cohort values pivoted
    """

    def requiresDS(self):
        return [inputdata.LotByCohortHsa]

    def run(self, i):
        df = i[inputdata.LotByCohortHsa]

        ptnt_cnt = df.groupBy(*hier_keys).agg(sum('ptnt_cnt').alias('ptnt_cnt'))
        pivoted = df.smvGroupBy(*hier_keys).smvPivotCoalesce(
            [['cohort']],
            by_cohort_vars,
            cohorts
        )

        return ptnt_cnt.smvJoinByKey(pivoted, hier_keys, 'inner')


class GeoDriver(SmvModule):
    """Filter Geos to keep the ones with siginficent ptnt_cnt"""
    def requiresDS(self):
        return [inputdata.GeoStdOfCare]
    def run(self, i):
        df = i[inputdata.GeoStdOfCare]

        # We will only report on HSA/HRRs with 100 or more patients
        return df.where(df.ptnt_cnt >= 100).select(
            hier_keys + ['ptnt_cnt']
        )


class GeoMaster(SmvModule, SmvOutput):
    """ geo level master
    """

    def requiresDS(self):
        return [
            GeoDriver,
            LotByHsaPivotCohort,
            inputdata.PddaStatsGeoLevel,
            inputdata.HsaHrrMaster,
            inputdata.PhyByHsa,
            account.AccountsByHsa
        ]

    def run(self, i):
        d = i[GeoDriver]
        sha = i[LotByHsaPivotCohort].drop('geo_name')
        pdda = i[inputdata.PddaStatsGeoLevel].drop('geo_name')
        phys = i[inputdata.PhyByHsa].drop('geo_name')
        survey = i[inputdata.HsaHrrMaster].drop('geo_name')
        acct = i[account.AccountsByHsa].drop('geo_name')

        keys = ['geo_type', 'geo_value']
        geomaster = d.smvJoinByKey(sha, keys, "leftouter"
            ).smvJoinByKey(pdda, keys, "leftouter"
            ).smvJoinByKey(phys, keys, "leftouter"
            ).smvJoinByKey(survey, keys, "leftouter"
            ).smvJoinByKey(acct, keys, "leftouter"
            )

        return geomaster

        # TODO: missing Hospital compare part
#        return geomaster.select(
#            "geo_type",                                   # hrr
#            "geo_value",                                  # 335
#            "geo_name",                                   # Youngstown, OH
#            "ptnt_cnt",                                   # 159
#            "avg_her_cmplnt_drtn_neoadjuvant",            # 103.52941176470588
#            "avg_her_cmplnt_drtn_adjuvant",               # 277.9259259259259
#            "avg_her_cmplnt_drtn_1L",                     # 319.5365853658537
#            "avg_her_cmplnt_drtn_2Lplus",                 # 246.61904761904762
#            "avg_per_cmplnt_drtn_neoadjuvant",            # 85.0625
#            "avg_per_cmplnt_drtn_adjuvant",               # 80.38461538461539
#            "avg_per_cmplnt_drtn_1L",                     # 239.6
#            "avg_per_cmplnt_drtn_2Lplus",                 # 276.0
#            "avg_kad_cmplnt_drtn_1L",                     # 107.33333333333333
#            "avg_kad_cmplnt_drtn_2Lplus",                 # 102.5
#            "Number_of_Physicians_InScope",               # 101  PDDA
#            "Average_Physician_Practice_Years_InScope",   # 29.66 PDDA (inscope)
#            "Average_GroupPractice_Size",                 # 35.896907216494846
#            "Pub_Cnt_Total",                              # 31
#            "Trail_Cnt_Total",                            # 7
#            "OpenPayment_Amt_Total",                      # 8951.9
#            "OpenPayment_Amt_Research",                   # 0.0
#            "OpenPayment_Amt_Speaker",                    # 0.0
#            "Average_Total_Physician_From_Any",           # 72.13131313131314
#            "SD_Total_Physician_From_Any",                # 55.00682170085109
#            "total_prmry_phy",                            # 14  SHA
#            "pct_phyn_gender_male",                       # 0.6428571428571429 SHA
#            "pop",                                        # 640491
#            "AllAge_incRate",                             # 157.14029080814564 cases per 100,000 population per year
#            "AllAge_scrRate",                             # 0.6688091511043871
#            "HospBedsPer1K",                              # 2.6538596
#            "RegNursesPer1K",                             # 4.614356
#            "HospEmplPer1K"                               # 16.723722
#        )

dev_all_vars_map = {
    "Number_of_Physicians_InScope": "Number of physicians",
    "Average_Physician_Practice_Years_InScope": "Average physicians' practice years",
    "Pub_Cnt_Total": "Total publication count",
    "Trail_Cnt_Total": "Total Clinical Trial count",
    "OpenPayment_Amt_Total": "Total OpenPayment amount",
    "OpenPayment_Amt_Consulting": "Total OpenPayment Consulting amount",
    "OpenPayment_Amt_Research": "Total OpenPayment Research amount",
    "OpenPayment_Amt_Speaker": "Total OpenPayment Speaker program amount",
    "Average_Total_Physician_From_Any": "Average size of physicians referral groups",
    "AllAge_scrRate": "Population Screening rate",
    "HospBedsPer1K": "Number of hospital bed per 1K people",
    "RegNursesPer1K": "Number of registered nurses per 1K people",
    "HospEmplPer1K": "Number of hospital employees per 1K people",
    "pctRprtQM": "Percent of physicians reports Quality measure",
    "pctUseEHR": "Percent of physicians use EHR system",
    "hasNatCompCancerNetwork": "Has National Comprehensive Cancer Network Member hospitals",
    "hasNCICancerCenter": "Has NCI-Designated Cancer Centers"
}

dev_all_vars = dev_all_vars_map.keys()

class GeoVarDevivation(SmvModule):
    """Calculate vars deviated from norm
    """
    def requiresDS(self):
        return [GeoMaster]

    def run(self, i):
        df = i[GeoMaster]

        all_vars = dev_all_vars

        all_vars_dbl = [col(c).cast("double").alias(c) for c in all_vars]

        def sd(cname):
            return sqrt(avg(col(cname) * col(cname)) - avg(col(cname)) * avg(col(cname)))

        avgs = [avg(c).alias(c + "_avg") for c in all_vars]
        stds = [sd(c).alias(c + "_std") for c in all_vars]

        avg_std = df.groupBy('geo_type').agg(*(avgs + stds))

        zscr = [((col(c) - col(c + "_avg"))/col(c + "_std")).alias(c + "_z") for c in all_vars]
        df_z = df.smvJoinByKey(avg_std, ['geo_type'], 'inner')\
            .where(col('geo_type') == 'hrr')\
            .na.fill(0)\
            .select(
                df.geo_value.alias('hrrnum'),
                *(all_vars_dbl + zscr)
            )

        return df_z


class GeoVarDevivationPilot(SmvModule):
    def requiresDS(self):
        return [GeoMaster, GeoVarDevivation]

    def run(self, i):
        m = i[GeoMaster]
        df = i[GeoVarDevivation]

        w = Window.partitionBy('hrrnum').orderBy(desc('value'))
        all_z = [c + "_z" for c in dev_all_vars]
        df_z = df.select('hrrnum', *all_z).smvUnpivot(*all_z)\
            .select(
                'hrrnum',
                regexp_replace(col('column'), '_z$', '').alias('column'),
                col('value').alias('zscr'),
                rank().over(w).alias('rank'))

        df_p = df.select('hrrnum', *dev_all_vars).smvUnpivot(*dev_all_vars)
        df_m = df_p.groupBy('column').agg(avg('value').alias('country_avg'))

        df_p_m = df_p.smvJoinByKey(df_m, ['column'], 'inner')

        res = df_p_m.smvJoinByKey(df_z, ['hrrnum', 'column'], 'inner').orderBy('hrrnum', 'rank')

        nvars = len(dev_all_vars)
        top3 = [1, 2, 3]
        bottom3 = [nvars, nvars - 1, nvars - 2]
        tb_map = {
            1: "top1",
            2: "top2",
            3: "top3",
            nvars: "bottom1",
            nvars - 1: "bottom2",
            nvars - 2: "bottom3"
        }

        all_vars_dbl = [col(c).cast("double").alias(c) for c in dev_all_vars]
        cntry = m.where(m.geo_type == 'country').select(*all_vars_dbl).smvUnpivot(*dev_all_vars)\
            .select('column', col('value').alias('country_value'))

        return res.smvJoinByKey(cntry, ['column'], 'inner').where(res.rank.isin(top3 + bottom3))\
            .withColumn('column_desc', applyMap(dev_all_vars_map, col('column')))\
            .withColumn('rank', applyMap(tb_map, col('rank')))\
            .drop('zscr')



class HsaMasterForUi(SmvModule, SmvOutput):
    """New Hsa master for Ui, only the vars to show
    """

    def requiresDS(self):
        return [GeoDriver, inputdata.GeoStdOfCare, inputdata.ZipHierMap]

    def run(self, i):
        d = i[GeoDriver].drop('ptnt_cnt')
        v = i[inputdata.GeoStdOfCare]
        z = i[inputdata.ZipHierMap]

        v_large_only = v.smvJoinByKey(d, hier_keys, 'inner').cache()

        all_vars = [
            "ptnt_pct_use_per_neoadjuvant",
            "avg_her_cmplnt_drtn_adjuvant",
            "ptnt_pct_use_per_1L",
            "ptnt_pct_use_kad_2L"
        ]

        v_max = v_large_only.agg(*[max(c).alias(c) for c in all_vars]).collect()[0].asDict()
        v_scrs = reduce(
            lambda df, c: df.withColumn(c + "_scr", col(c)/v_max[c]),
            all_vars,
            v_large_only
        )

        raw_scr_col = reduce(lambda x, y: x + y, [col(c + "_scr") for c in all_vars])
        res = v_scrs\
            .withColumn('full_scr', raw_scr_col/4)\
            .smvSelectMinus(*[c + "_scr" for c in all_vars])

        with_parent = hier_utils.add_parent_keys(res, z, "hsa")

        return with_parent.smvDesc(
            ('full_scr', 'Score: Variation of Care Score'),
            ('ptnt_pct_use_per_neoadjuvant', 'Neoadjuvant: Percent of patients using Perjeta in Neoadjuvant setting'),
            ('avg_her_cmplnt_drtn_adjuvant', 'Adjuvant: Average Herceptin compliance duration in Adjuvant setting'),
            ('ptnt_pct_use_per_1L', '1st Line Metastatic: Percent of patients using Perjeta in 1st line Metastatic setting'),
            ('ptnt_pct_use_kad_2L', '2nd Line Metastatic: Percent of patients using Kadcyla in 2nd line Metastatic setting')
        )

class HrrAnalysis(SmvModule):
    """
    """
    def requiresDS(self):
        return [GeoVarDevivation, HsaMasterForUi]

    def run(self, i):
        v = i[GeoVarDevivation]
        m = i[HsaMasterForUi]

        return m.where(m.geo_type == "hrr").join(v, m.geo_value == v.hrrnum, 'inner')

class ByHasWithMMM(SmvModule, SmvOutput):
    """Prepare for UI
    """

    def requiresDS(self):
        return [LotByHsaPivotCohort, inputdata.ZipHierMap]

    def run(self, i):
        pivoted = i[LotByHsaPivotCohort]
        zipmap = i[inputdata.ZipHierMap]

        df_all = hier_utils.add_parent_keys(pivoted, zipmap, 'hsa').cache()

        all_vars = str_list_dot(by_cohort_vars, cohorts)

        minmax_cols = flatten([
            [max(col(c)).alias(c + "_max"), min(col(c)).alias(c + "_min") ] for c in all_vars
        ])

        dict_mm = df_all.agg(*minmax_cols).collect()[0].asDict()
        dict_mid = df_all.where(col('geo_type') == "country").collect()[0].asDict()

        add_mmm = [lit(dict_mm[v]).cast("float").alias(v) for v in [v0 + m for v0 in all_vars for m in ["_max", "_min"]]]\
             + [lit(dict_mid[v]).cast("float").alias(v + "_mid") for v in all_vars]

        res = df_all.smvSelectPlus(*add_mmm)
        df_all.unpersist()

        return res
