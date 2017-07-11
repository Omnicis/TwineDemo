from smv import *
from pyspark.sql.functions import *
import inputdata, refinput

class AllClaims(SmvModule, SmvRunConfig, SmvOutput):
    """
    All RxPxDx claims for all inscope PTNTs
    """

    def requiresDS(self):
        return [inputdata.PtntTumor, inputdata.PxDxRxONC]

    def run(self, i):
        df = i[inputdata.PxDxRxONC]
        ptnt = i[inputdata.PtntTumor]

        startyear = int(self.smvGetRunConfig('startyear'))
        endyear = int(self.smvGetRunConfig('endyear'))

        return df.where(
            (col('clm_sts').isNull()) |
            (col('clm_sts') == "") |
            (col('clm_sts') == "1")
        ).withColumn('year', df.dt.smvYear()
        ).where((col('year') >= startyear) & (col('year') <= endyear)
        ).smvJoinByKey(ptnt, ["ptnt_gid"], "inner"
        ).withColumn("diagArr",
            array(
                "diag0", "diag1", "diag2", "diag3", "diag4", "diag5",
                "diag6", "diag7", "diag8", "diag9", "diag10", "diag11"
            )
        ).select(
            'clm_gid',     ## clm_gid + dt + svc_ln_seq is the unique PK of claim table
            'dt',
            'svc_ln_seq',
            'ptnt_gid',
            'prodt',
            'loc_cde',
            'diagArr',
            'phyn_key',
            'phyn_gid',
            'clm_typ',
            'clm_sts',
            'day_sply',
            'drg_id',
            'adj_svc_unit_nbr',
            'prodt_strgth',
            'qty',
            'dt_mth'
        ).smvDedupByKey('clm_gid', 'dt', 'svc_ln_seq')


class AllProdts(SmvModule, SmvOutput):
    """
    Inscope Prodts including drugs NCD11 code and proc codes
    """

    def requiresDS(self):
        return [inputdata.DrugMaster, refinput.Drugs, refinput.Procs]

    def run(self, i):
        dMaster = i[inputdata.DrugMaster]
        drugs = i[refinput.Drugs]
        procs = i[refinput.Procs]

        #Drugs NCD11 codes lookup in drug master table
        drugCode = drugs.join(dMaster,
                (upper(dMaster.drg_nm).startswith(drugs.drugName)) | \
                (upper(dMaster.drg_gnrc_nm).startswith(drugs.drugName)) | \
                (upper(dMaster.drg_nm).startswith(drugs.genericName)) | \
                (upper(dMaster.drg_gnrc_nm).startswith(drugs.genericName)), \
                "inner"
            ).select(
                col("ndc11").alias("prodt"),
                col("drugName").alias("prodt_cd"), #TODO: prodt_cd could be shorter
                lit('Drug').alias("targetType"),
                col("type").alias("drugType")
            )

        #Drugs JCodes
        drugJcode = drugs.select(
            explode(col('jCode')).alias("prodt"),
            col("drugName").alias("prodt_cd"),
            lit('Drug').alias("targetType"),
            col("type").alias("drugType")
        )

        #Procedures: tests, screens and surgeries
        procsOther = procs.select(
            procs.procCode.alias('prodt'),
            lit('Other').alias('prodt_cd'),
            lit('Procedure').alias("targetType"),
            procs.procType,
            procs.subType.alias("procSubType")
        )

        return procsOther.smvUnion(drugCode, drugJcode).smvDedupByKey('prodt')


class FilteredClaims(SmvModule, SmvOutput):
    """
    All claims with in-scope prodt (including drugs and procs)
    """

    def requiresDS(self):
        return [AllClaims, AllProdts, refinput.Diags]

    def run(self, i):
        df = i[AllClaims]
        pd = i[AllProdts]
#        diagCode = i[refinput.Diags]

#        diagClct = diagCode.select("diagCode").distinct().collect()
#        diagList = [r[0] for r in diagClct]

        filterList = [r[0] for r in pd.select('prodt').distinct().collect()]

        dfFilter = df.where(
                col("diagArr").smvIsAnyIn(*BreastCancer) | \
                col("prodt").isin(filterList)
            )

        return dfFilter.smvJoinByKey(pd, ['prodt'], 'leftouter')


class AfflUniq(SmvModule):
    """
    """
    def requiresDS(self):
        return [inputdata.Affl]

    def run(self, i):
        df = i[inputdata.Affl]

        # affl_typ_flg has 3 values:
        # Histogram of affl_typ_flg: String sort by Key
        # key                      count      Pct    cumCount   cumPct
        # PRIMARY                  43508   51.12%       43508   51.12%
        # SECONDARY                33421   39.27%       76929   90.39%
        # UNKNOWN                   8183    9.61%       85112  100.00%
        f = df.where(df.affl_typ_flg.isin(["PRIMARY"]))

        # One mdm_key could have multiple records (very rare though)
        return f.withColumn(
            "dt",
            coalesce(f.lvl1_soc_340b_strt_dt, lit("19700101").smvStrToTimestamp("yyyyMMdd"))
        ).smvDedupByKeyWithOrder("mdm_key")(
            col("dt").desc()
        ).smvSelectMinus(
            "dt",
            "affl_typ_flg",
            "fld_frc_cd",
            "ld_tm"
        )


class AllAccts(SmvModule, SmvOutput):
    """
    """
    def requiresDS(self):
        return [inputdata.Affl]

    def run(self, i):
        df = i[inputdata.Affl]

        lvl1 = [x for x in df.columns if x.startswith("lvl1")]

        # collect all the level 1 orgs
        # since lvl1_org_id is not unique, and for duplicate records, there are typically
        # 1 with non-null lvl1_msa_cd
        return df.select(*lvl1).distinct()\
            .withColumn('lvl1_msa_cd_nn', coalesce(df.lvl1_msa_cd, lit("")))\
            .smvDedupByKeyWithOrder('lvl1_org_id')(col('lvl1_msa_cd_nn').desc())


class AllPhyns(SmvModule, SmvOutput):
    """
    All in-scope physicians with there dim info and affiliation info (primary affl only)
    """
    def requiresDS(self):
        return [FilteredClaims, inputdata.PhynDim, AfflUniq]

    def run(self, i):
        df = i[FilteredClaims]
        ph = i[inputdata.PhynDim].select(
            "phyn_gid",
            "phyn_fst_nm",
            "phyn_mid_nm",
            "phyn_last_nm",
            "phyn_gender",
            "phyn_dob",
            "phyn_addr",
            "phyn_city",
            "phyn_st",
            "phyn_zip_cde",
            "mdm_id",
            "mdm_npi",
            "mdm_prim_specialty_code",
            "mdm_sec_specialty_code"
        ).smvDedupByKey("phyn_gid")

        affl = i[AfflUniq].withColumnRenamed("mdm_key", "mdm_id")

        return df.select('phyn_gid').distinct().coalesce(16).smvJoinByKey(
                ph, ['phyn_gid'], 'leftouter'
            ).smvJoinByKey(affl, ['mdm_id'], 'leftouter')

class PtntLoTRaw(SmvModule, SmvOutput):
    """
    """
    def requiresDS(self):
        return [inputdata.PtntLoT]

    def run(self, i):
        return i[inputdata.PtntLoT]


class LotTopAccount(SmvModule, SmvOutput):
    """
    Top accounts with LOT data, number of pt as rank
    """
    def requiresDS(self):
        return [inputdata.PtntLoT, AllPhyns]

    def run(self, i):
        df = i[inputdata.PtntLoT]
        aff = i[AllPhyns].select("phyn_gid", "lvl1_org_id")

        lotAt = df.select("ptnt_gid", "prmry_phy_lot"
            ).smvRenameField(("prmry_phy_lot", "phyn_gid")
            ).smvJoinByKey(aff, ["phyn_gid"], "leftouter")

        return lotAt.filter(col("lvl1_org_id").isNotNull()
            ).groupBy("lvl1_org_id"
            ).agg(countDistinct("ptnt_gid").alias('NumberOfPt')
            ).orderBy(col("NumberOfPt").desc()).limit(100)


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
