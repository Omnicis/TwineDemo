from smv import *
import inputdata
from hsamap import GeoDriver
from pyspark.sql.functions import *
from magbc.core import hier_utils, hier_keys


class Her2Accounts(SmvModule):
    """Inscope accounts from SHA lot data
    """

    def requiresDS(self):
        return [inputdata.LoT, inputdata.AllPhyns]

    def run(self, i):
        lot = i[inputdata.LoT]
        phyns = i[inputdata.AllPhyns]

        inscopePhys = lot.select(
            "prmry_phy_lot"
            ).smvDedupByKey("prmry_phy_lot"
            ).smvRenameField(("prmry_phy_lot", "phyn_gid"))

        account = phyns.smvJoinByKey(inscopePhys, ["phyn_gid"], "inner"
          ).groupBy("lvl1_org_id"
          ).agg(
          sum(lit(1)).alias("number_of_phyns"),
          first("lvl1_org_nm").alias("lvl1_org_nm"),
          first("lvl1_dba_nm").alias("lvl1_dba_nm"),
          first("lvl1_physical_addr_1").alias("lvl1_physical_addr_1"),
          first("lvl1_physical_addr_2").alias("lvl1_physical_addr_2"),
          first("lvl1_physical_city").alias("lvl1_physical_city"),
          first("lvl1_physical_state").alias("lvl1_physical_state"),
          first("lvl1_physical_zip").alias("lvl1_physical_zip"),
          first("lvl1_cot_classification").alias("lvl1_cot_classification"),
          first("lvl1_cot_facility_type").alias("lvl1_cot_facility_type"),
          first("lvl1_cot_specialty").alias("lvl1_cot_specialty"),
          first("lvl1_teaching_hosp").alias("lvl1_teaching_hosp"),
          first("lvl1_resident_program").alias("lvl1_resident_program"),
          first("lvl1_hin").alias("lvl1_hin"),
          first("lvl1_dea").alias("lvl1_dea"),
          first("lvl1_formulary").alias("lvl1_formulary"),
          first("lvl1_status_indicator").alias("lvl1_status_indicator"),
          first("lvl1_flg_340b").alias("lvl1_flg_340b"),
          first("lvl1_msa_cd").alias("lvl1_msa_cd"),
          first("lvl1_soc_flg_340b").alias("lvl1_soc_flg_340b"),
          first("lvl1_soc_final_cls").alias("lvl1_soc_final_cls"),
          first("lvl1_soc_final_grp_desc").alias("lvl1_soc_final_grp_desc"),
          first("lvl1_soc_final_grp_cd").alias("lvl1_soc_final_grp_cd"),
          first("lvl1_soc_340b_strt_dt").alias("lvl1_soc_340b_strt_dt"),
          first("lvl1_soc_340b_end_dt").alias("lvl1_soc_340b_end_dt")
          )

        return account


class AccountsMaster(SmvModule):
    """Account level master from SHA data
    """

    def requiresDS(self):
        return [Her2Accounts,
        inputdata.HcosCcnMatch,
        inputdata.HospCompare,
        inputdata.AcctFlags,
        inputdata.ZipMaster
        ]

    def run(self, i):
        act = i[Her2Accounts]
        hsp = i[inputdata.HospCompare]
        zip = i[inputdata.ZipMaster]
        flag = i[inputdata.AcctFlags].smvRenameField(("ORGANIZATION_ID", "lvl1_org_id"))
        matcher = i[inputdata.HcosCcnMatch].select("CCN", "Org_ID"
          ).smvRenameField(("Org_ID", "lvl1_org_id"))

        return act.smvJoinByKey(matcher, ["lvl1_org_id"], "leftouter"
          ).smvJoinByKey(hsp, ["CCN"], "leftouter"
          ).smvJoinByKey(flag, ["lvl1_org_id"], "leftouter"
          ).withColumn("zip", col("lvl1_physical_zip").substr(1, 5)
          ).smvJoinByKey(zip, ["zip"], "leftouter")


class AccountsByHsa(SmvModule):
    """Aggregate AccountsMaster to HSA/HRR"""
    def requiresDS(self):
        return [AccountsMaster, inputdata.ZipHierMap, inputdata.AcctFlags]

    def run(self, i):
        df = i[AccountsMaster]
        zipmap = i[inputdata.ZipHierMap]
        flag = i[inputdata.AcctFlags]

        flag_agg = hier_utils.hier_agg(
            flag,
            zipmap,
            'hsa', 'hrr', 'country'
        )(
            (sum('isCommissionOnCancer') > 0).alias('hasCommissionOnCancer'),
            (sum('isOncCareModel') > 0).alias('hasOncCareModel'),
            (sum('isNatCompCancerNetwork') > 0).alias('hasNatCompCancerNetwork'),
            (sum('isNCICancerCenter') > 0).alias('hasNCICancerCenter'),
        ).withColumn('hasCommissionOnCancer', when(col('hasCommissionOnCancer'), lit(1)).otherwise(lit(0))
        ).withColumn('hasOncCareModel', when(col('hasOncCareModel'), lit(1)).otherwise(lit(0))
        ).withColumn('hasNatCompCancerNetwork', when(col('hasNatCompCancerNetwork'), lit(1)).otherwise(lit(0))
        ).withColumn('hasNCICancerCenter', when(col('hasNCICancerCenter'), lit(1)).otherwise(lit(0))
        )

        res = hier_utils.hier_agg(
            df,
            zipmap,
            'hsa', 'hrr', 'country'
        )(
            avg(when(col('Able_To_Receive_eLab_Results'), lit(1)).otherwise(lit(0))).alias('avg_Able_To_Receive_eLab_Results'),
            avg(when(col('Able_To_Track_Lab_Results'), lit(1)).otherwise(lit(0))).alias('avg_Able_To_Track_Lab_Results'),
            avg('H_HSP_RATING_STAR_RATING').alias('avg_H_HSP_RATING_STAR_RATING'),
            avg('Total_Performance_Score').alias('avg_Total_Performance_Score')
        )

        return res.smvJoinByKey(flag_agg, hier_keys, 'leftouter')

class AcctMasterForUi(SmvModule, SmvOutput):
    """Ui specific Account variables
    """

    def requiresDS(self):
        return [AccountsMaster, GeoDriver]

    def run(self, i):
        df = i[AccountsMaster]
        geo = i[GeoDriver]

        hrr_list = geo.where(geo.geo_type == 'hrr').select(
            geo.geo_value.alias('hrrnum')
        )

        # Accounts with enough SHA physicians
        return df.where(df.number_of_phyns >= 3)\
            .select(
                df.lvl1_org_id,
                df.number_of_phyns,
                df.lvl1_org_nm.alias('acct_name'),
                df.lvl1_cot_classification.alias('acct_classification'),
                df.hsanum,
                df.hrrnum,
                df.latitude,
                df.longitude
            ).smvJoinByKey(hrr_list, ['hrrnum'], 'inner')
