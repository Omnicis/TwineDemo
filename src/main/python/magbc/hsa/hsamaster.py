from smv import *
from pyspark.sql.functions import *

import inputdata
import hsacapacity
import hsainsurance
import hsaincidence
import compare
from twinelib.hier import HierarchyUtils
from magbc.core import *

class HsaMaster(SmvModule):
    """ Combine all hsa tables"""
    def requiresDS(self):
        return [
            inputdata.ZipMaster,
            hsainsurance.HSAUninsured,
            hsainsurance.HSAUnderinsured,
            hsaincidence.HSAIncidence,
            hsacapacity.HSACapacity,
            compare.HsaPhysComp
        ]

    def run(self, i):
        zipM = i[inputdata.ZipMaster]
        cap = i[hsacapacity.HSACapacity]
        under = i[hsainsurance.HSAUnderinsured]
        unins = i[hsainsurance.HSAUninsured]
        inc = i[hsaincidence.HSAIncidence]
        phys = i[compare.HsaPhysComp]

        hsa = zipM.where(col('hsanum').isNotNull()
        ).groupBy('hsanum').agg(
            first('hrrnum').alias('hrrnum'),
            first('hsacity').alias('hsacity'),
            first('hrrstate').alias('hrrstate'),
            sum(col('EstimatedPopulation')).alias('Population')
        ).coalesce(1)

        return hsa.smvJoinMultipleByKey(['hsanum'], 'LeftOuter'
        ).joinWith(inc, '_inc'
        ).joinWith(unins, '_unins'
        ).joinWith(under, '_undins'
        ).joinWith(cap, '_cap'
        ).joinWith(phys, '_phys'
        ).doJoin().select(
            'hsanum',
            'pop',
            'AllAge_incRate',
            'AllAge_scrRate',
            #'65Plus_incRate',
            'Medicaid_Rate',
            'Uninsured_Rate',
            'Underinsured_Rate',
            'Resident_Population_2010',
            col('Acute_Care_Hospital_Beds_per_1_000_Residents_2012').alias('HospBedsPer1K'),
            col('Hospital_based_Registered_Nurses_per_1_000_Residents_2012').alias('RegNursesPer1K'),
            col('FTE_Hospital_Employees_per_1_000_Residents_2012').alias('HospEmplPer1K'),
            'Number_of_Physicians_Total',
            'avg_yrs_of_practice',
            col('Avg_Number_of_Group_Practice_members').alias('avgNumGrpPrac'),
            col('Pct_Professional_accepts_Medicare_Assignment').alias('pctAccptMedicare'),
            col('Pct_Reported_Quality_Measures').alias('pctRprtQM'),
            col('Pct_Used_electronic_health_records').alias('pctUseEHR'),
            col('Pct_Participated_in_the_Medicare_Maintenance_of_Certification_Program').alias('pctMMCP'),
            col('Pct_Committed_to_heart_health_through_the_Million_Hearts_initiative').alias('pctMHI'),
            col('avg_Screening_for_depression_and_developing_a_follow_up_plan').alias('avgScrDepression'),
            col('avg_Screening_for_tobacco_use_and_providing_help_quitting_when_needed').alias('avgScrTobacco'),
            col('avg_Screening_for_an_unhealthy_body_weight_and_developing_a_follow_up_plan').alias('avgScrWeight'),
            col('avg_Screening_for_high_blood_pressure_and_developing_a_follow_up_plan').alias('avgScrHiBldPres'),
            col('avg_Comparing_new_and_old_medications').alias('avgCompMed'),
            col('avg_Using_aspirin_or_prescription_medicines_to_reduce_heart_attacks_and_strokes').alias('avgUseAspirin')
        )


class HsaHierMap(SmvModule):
    """Standardize ZipMaster for hierarchy aggregation
    """

    def requiresDS(self):
        return [inputdata.ZipMaster]

    def run(self, i):
        z = i[inputdata.ZipMaster]

        return z.filter(col("hsanum").isNotNull()
            ).groupBy('hsanum').agg(
            first('hrrnum').alias('hrr'),
            first('hsacity').alias('hsa_name'),
            first('hrrcity').alias('hrr_name')
            ).withColumn("hsa", col("hsanum")
            ).withColumn("country", lit('US')
            ).withColumn("country_name", lit('US'))


class HrrHierMap(SmvModule):
    """Standardize ZipMaster for hierarchy aggregation
    """

    def requiresDS(self):
        return [inputdata.ZipMaster]

    def run(self, i):
        z = i[inputdata.ZipMaster]

        return z.filter(col("hrrnum").isNotNull()
            ).groupBy('hrrnum').agg(
            first('hrrcity').alias('hrr_name')
            ).withColumn("hrr", col("hrrnum")
            ).withColumn("country", lit('US')
            ).withColumn("country_name", lit('US'))


class HrrAgg(SmvModule):
    """aggregate from hrr up to country"""
    def requiresDS(self):
        return [
            inputdata.CancerPatientCount,
            HrrHierMap
        ]

    def run(self, i):
        cpc = i[inputdata.CancerPatientCount]
        hrrmap = i[HrrHierMap]

        hrr = cpc.where((col('geo_type') == 'HRR') & (col('year') == '2013')
        ).select(
            col('geo_value').alias("hrrnum"),
            (col('Percent_of_Medicare_beneficiaries_with_breast_cancer') / lit(100)).alias('65Plus_prevalence')
        )

        hrr_agg = hier_utils_hrr.hier_agg(
            hrr,
            hrrmap,
            'hrr', 'country'
        )(avg(col("65Plus_prevalence")
            ).alias("65Plus_prevalence"))

        return hrr_agg


class HsaAgg(SmvModule):
    """aggregate from hsa up to hrr and country"""
    def requiresDS(self):
        return [
            HsaMaster,
            HsaHierMap
        ]

    def run(self, i):
        hsa = i[HsaMaster]
        hsamap = i[HsaHierMap]

        hsa_agg = hier_utils_hsa.hier_agg(
            hsa,
            hsamap,
            'hsa', 'hrr', 'country'
        )(sum('pop').alias('pop'),
            scaleRate(col('AllAge_incRate'), col('pop')).alias('AllAge_incRate'),
            scaleRate(col('AllAge_scrRate'), col('pop')).alias('AllAge_scrRate'),
            #(sum(col('65Plus_incRate') * col('pop')) / sum('pop')).alias('hrr_65Plus_incRate'),
            scaleRate(col('HospBedsPer1K'), col('Resident_Population_2010')).cast("float").alias('HospBedsPer1K'),
            scaleRate(col('RegNursesPer1K'), col('Resident_Population_2010')).cast("float").alias('RegNursesPer1K'),
            scaleRate(col('HospEmplPer1K'), col('Resident_Population_2010')).cast("float").alias('HospEmplPer1K'),
            scaleRate(col('avg_yrs_of_practice'), col('pop')).alias('avg_yrs_of_practice'),
            scaleRate(col('avgNumGrpPrac'), col('pop')).alias('avgNumGrpPrac'),
            scaleRate(col('pctAccptMedicare'), col('pop')).alias('pctAccptMedicare'),
            scaleRate(col('pctRprtQM'), col('pop')).alias('pctRprtQM'),
            scaleRate(col('pctUseEHR'), col('pop')).alias('pctUseEHR'),
            scaleRate(col('pctMMCP'), col('pop')).alias('pctMMCP'),
            scaleRate(col('pctMHI'), col('pop')).alias('pctMHI'),
            scaleRate(col('avgScrDepression'), col('pop')).alias('avgScrDepression'),
            scaleRate(col('avgScrTobacco'), col('pop')).alias('avgScrTobacco'),
            scaleRate(col('avgScrWeight'), col('pop')).alias('avgScrWeight'),
            scaleRate(col('avgScrHiBldPres'), col('pop')).alias('avgScrHiBldPres'),
            scaleRate(col('avgCompMed'), col('pop')).alias('avgCompMed'),
            scaleRate(col('avgUseAspirin'), col('pop')).alias('avgUseAspirin'))

        return hsa_agg

class HsaHrrMaster(SmvModule, SmvOutput):
    """aggregate to hrr level stats and join these to hsa level table"""
    def requiresDS(self):
        return [
            HsaAgg,
            HrrAgg
        ]

    def run(self, i):
        hsa = i[HsaAgg]
        hrr = i[HrrAgg]

        return hsa.smvJoinByKey(hrr, hier_keys, "outer")
