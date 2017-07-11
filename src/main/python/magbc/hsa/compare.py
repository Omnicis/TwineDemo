###---GLOBAL_IMPORTS_START---###
from smv import *
from pyspark.sql.functions import *
###---GLOBAL_IMPORTS_END---###
###---PhysicianCompare_IMPORTS_START---###
import magbc.hsa.inputdata
import magbc.hsa.inputdata


###---PhysicianCompare_IMPORTS_END---###
from smv import *
from pyspark.sql.functions import *
from magbc.core import *

import inputdata

class HospCompare(SmvModule, SmvOutput):
    """ CCN level measures of hospitals"""
    def requiresDS(self):
        return [inputdata.CmsHCMeasure]

    def run(self, i):
        df = i[inputdata.CmsHCMeasure]

        return df.select(
            col('Provider_ID').alias('CCN'),
            'Hospital_Type',
            col('OP_12').alias('Able_To_Receive_eLab_Results'),
            col('OP_17').alias('Able_To_Track_Lab_Results'),
            'H_HSP_RATING_STAR_RATING',
            col('MSPB_1').alias('Per_Patient_Medicare_Spend'),
            'Total_Performance_Score',
            'Weighted_Clinical_Process_of_Care_Domain_Score',
            'Weighted_Patient_Experience_of_Care_Domain_Score',
            'Weighted_Outcome_Domain_Score',
            'Weighted_Efficiency_Domain_Score',
            'Read_30_Rate_Medical',
            'Read_30_Rate_Surgical'
        )


class GroupCompare(SmvModule):
    """ pac id level measures """
    def requiresDS(self):
        return [
            inputdata.GroupPracticePE,
            inputdata.GroupPracticeCQoC
        ]

    def run(self, i):
        df = i[inputdata.GroupPracticeCQoC]
        pe = i[inputdata.GroupPracticePE]

        return df.smvJoinByKey(pe, ['Group_Practice_PAC_ID'], 'LeftOuter'
        ).select(
            col('Group_Practice_PAC_ID').alias('PAC_ID'),
            'Participating_in_PQRS',
            'Screening_for_tobacco_use_and_providing_help_quitting_when_needed',
            'Screening_for_high_blood_pressure_and_developing_a_follow_up_plan',
            'Screening_for_breast_cancer',
            'Screening_for_colorectal_colon_or_rectum_cancer',
            'Getting_timely_care_appointments_and_information',
            'How_well_health_care_professionals_communicate',
            'Health_promotion_and_education',
            'Patients_rating_of_doctors',
            'Courteous_and_helpful_office_staff',
            'Health_care_professionals_working_together_for_your_care',
            'Between_visit_communication',
            'Attention_to_patient_medication_cost'
        )


class PhysicianCompare(SmvModule):
    """
    npi level measures
    """

    def requiresDS(self):
        return [magbc.hsa.inputdata.PhysicianCQoC, magbc.hsa.inputdata.PhysicianCompare]

    def run(self, i):
        pc = i[inputdata.PhysicianCompare]
        cqoc = i[inputdata.PhysicianCQoC]
        
        phys = pc.select(
            'NPI',
            'PAC_ID',
            #'Credential',
            #'Medical_school_name',
            'Graduation_year',
            'Primary_specialty',
            'Group_Practice_PAC_ID',
            'Number_of_Group_Practice_members',
            col('Zip_Code').alias('zip'),
            'Hospital_affiliation_CCN_1',
            'Professional_accepts_Medicare_Assignment',
            'Reported_Quality_Measures',
            'Used_electronic_health_records',
            'Participated_in_the_Medicare_Maintenance_of_Certification_Program',
            'Committed_to_heart_health_through_the_Million_Hearts_initiative'
        )
        
        res = phys.smvJoinByKey(cqoc, ['NPI'], 'leftouter')
        
        return res
    def isEphemeral(self):
        return False


class PhysCompAcct(SmvModule):
    """ account level physician measures"""
    def requiresDS(self):
        return [PhysicianCompare]

    def run(self, i):
        df = i[PhysicianCompare]

        phys = df.groupBy('Group_Practice_PAC_ID').agg(

            avg(col('Graduation_year')).alias('avgGradYear'),
            #(sum(col('Graduation_year')) / col('numPhys')).alias('avgGradYear'),
            first('Number_of_Group_Practice_members').alias('Number_of_Group_Practice_members'),

            (sum(when(col('Professional_accepts_Medicare_Assignment') == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1))).alias('Pct_Professional_accepts_Medicare_Assignment'),
            #avg('Professional_accepts_Medicare_Assignment'),
            (sum(when(col('Reported_Quality_Measures') == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1))).alias('Pct_Reported_Quality_Measures'),
            (sum(when(col('Used_electronic_health_records') == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1))).alias('Pct_Used_electronic_health_records'),
            (sum(when(col('Participated_in_the_Medicare_Maintenance_of_Certification_Program') == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1))).alias('Pct_Participated_in_the_Medicare_Maintenance_of_Certification_Program'),
            (sum(when(col('Committed_to_heart_health_through_the_Million_Hearts_initiative') == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1))).alias('Committed_to_heart_health_through_the_Million_Hearts_initiative'),
            avg('Screening_for_depression_and_developing_a_follow_up_plan').alias('avg_Screening_for_depression_and_developing_a_follow_up_plan'),
            avg('Screening_for_tobacco_use_and_providing_help_quitting_when_needed').alias('avg_Screening_for_tobacco_use_and_providing_help_quitting_when_needed'),
            avg('Screening_for_an_unhealthy_body_weight_and_developing_a_follow_up_plan').alias('avg_Screening_for_an_unhealthy_body_weight_and_developing_a_follow_up_plan'),
            avg('Screening_for_high_blood_pressure_and_developing_a_follow_up_plan').alias('avg_Screening_for_high_blood_pressure_and_developing_a_follow_up_plan'),
            avg('Comparing_new_and_old_medications').alias('avg_Comparing_new_and_old_medications'),
            avg('Using_aspirin_or_prescription_medicines_to_reduce_heart_attacks_and_strokes').alias('avg_Using_aspirin_or_prescription_medicines_to_reduce_heart_attacks_and_strokes')
        )

        return phys


class HsaPhysComp(SmvModule):
    def requiresDS(self):
        return [
            PhysicianCompare,
            inputdata.ZipMaster
        ]

    def run(self, i):
        df = i[PhysicianCompare]
        zipM = i[inputdata.ZipMaster].select('zip', 'hsanum', 'hrrnum')

        physzip = df.smvJoinByKey(zipM, ['zip'], 'inner')

        def flagavg(col):
            return (sum(when(col == 'Y', lit(1)).otherwise(lit(0))) / sum(lit(1)))

        phys = physzip.groupBy('hsanum').agg(
            sum(lit(1)).alias("Number_of_Physicians_Total"),
            avg(lit(2016) - col('Graduation_year')).alias('avg_yrs_of_practice'),
            avg(col('Number_of_Group_Practice_members')).alias('Avg_Number_of_Group_Practice_members'),
            flagavg(col('Professional_accepts_Medicare_Assignment')).alias('Pct_Professional_accepts_Medicare_Assignment'),
            flagavg(col('Reported_Quality_Measures')).alias('Pct_Reported_Quality_Measures'),
            flagavg(col('Used_electronic_health_records')).alias('Pct_Used_electronic_health_records'),
            flagavg(col('Participated_in_the_Medicare_Maintenance_of_Certification_Program')).alias('Pct_Participated_in_the_Medicare_Maintenance_of_Certification_Program'),
            flagavg(col('Committed_to_heart_health_through_the_Million_Hearts_initiative')).alias('Pct_Committed_to_heart_health_through_the_Million_Hearts_initiative'),
            avg('Screening_for_depression_and_developing_a_follow_up_plan').alias('avg_Screening_for_depression_and_developing_a_follow_up_plan'),
            avg('Screening_for_tobacco_use_and_providing_help_quitting_when_needed').alias('avg_Screening_for_tobacco_use_and_providing_help_quitting_when_needed'),
            avg('Screening_for_an_unhealthy_body_weight_and_developing_a_follow_up_plan').alias('avg_Screening_for_an_unhealthy_body_weight_and_developing_a_follow_up_plan'),
            avg('Screening_for_high_blood_pressure_and_developing_a_follow_up_plan').alias('avg_Screening_for_high_blood_pressure_and_developing_a_follow_up_plan'),
            avg('Comparing_new_and_old_medications').alias('avg_Comparing_new_and_old_medications'),
            avg('Using_aspirin_or_prescription_medicines_to_reduce_heart_attacks_and_strokes').alias('avg_Using_aspirin_or_prescription_medicines_to_reduce_heart_attacks_and_strokes')
        )

        return phys
