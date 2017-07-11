from smv import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import inputdata
from twinelib.hier import HierarchyUtils
from magbc.core import hier_utils

def sd(cname): 
    return sqrt(avg(col(cname) * col(cname)) - avg(col(cname)) * avg(col(cname)))

class PddaStatsGeoLevel(SmvModule, SmvOutput):
    """
    Public Domain stats agg to geo level
    """
    def requiresDS(self):
        return[inputdata.KolMaster, 
        inputdata.ZipHierMap,
        inputdata.PhysicianCompare]

    def run(self, i):
        zipmap = i[inputdata.ZipHierMap]
        pc = i[inputdata.PhysicianCompare].select("NPI", "Graduation_year")
        pdda = i[inputdata.KolMaster].\
        withColumn("zip", col("PECOS_Zip_Code").substr(1, 5))

        def cap_to_onehundred(c):
            return when(c > 100.0, 100.0).otherwise(c)   

        pddafull = pdda.smvJoinByKey(pc, ["NPI"], "leftouter")     

        pdda_agged = hier_utils.hier_agg(
            pddafull,
            zipmap,
            'hsa', 'hrr', 'country'
        )(  sum(lit(1)).alias("Number_of_Physicians_InScope"),
            avg(lit(2016) - col('Graduation_year')).alias('Average_Physician_Practice_Years_InScope'),
            #group practice size
            avg("Number_of_Group_Practice_members").alias("Average_GroupPractice_Size"),
            #pt stats
            sum("Patient_Cnt_InScope").alias("Sum_Patient_Cnt_InScope"),
            avg("Patient_Cnt_InScope").alias("Average_Patient_Cnt_InScope"),
            sum("Patient_Cnt_Total").alias("Sum_Patient_Cnt_Total"),
            avg("Patient_Cnt_Total").alias("Average_Patient_Cnt_Total"),
            avg("Patient_Cancer_Pct").alias("Average_Patient_Cancer_Pct"),
            cap_to_onehundred(avg("Patient_InScope_Pct")).alias("Average_Patient_InScope_Pct"),
            #pubmed
            sum("Pub_Cnt_Total").alias("Pub_Cnt_Total"),
            #clinical trials
            sum("Trail_Cnt_Total").alias("Trail_Cnt_Total"),
            #open payments
            sum("OpenPayment_Cnt_Total").alias("OpenPayment_Cnt_Total"),
            sum("OpenPayment_Amt_Total").alias("OpenPayment_Amt_Total"),
            sum("OpenPayment_Cnt_Consulting").alias("OpenPayment_Cnt_Consulting"),
            sum("OpenPayment_Cnt_OtherGeneral").alias("OpenPayment_Cnt_OtherGeneral"),
            sum("OpenPayment_Cnt_Research").alias("OpenPayment_Cnt_Research"),
            sum("OpenPayment_Cnt_Speaker").alias("OpenPayment_Cnt_Speaker"),
            sum("OpenPayment_Amt_Consulting").alias("OpenPayment_Amt_Consulting"),
            sum("OpenPayment_Amt_OtherGeneral").alias("OpenPayment_Amt_OtherGeneral"),
            sum("OpenPayment_Amt_Research").alias("OpenPayment_Amt_Research"),
            sum("OpenPayment_Amt_Speaker").alias("OpenPayment_Amt_Speaker"),
            #referrals
            avg("Total_Physician_From_Any").alias("Average_Total_Physician_From_Any"),
            sd("Total_Physician_From_Any").alias("SD_Total_Physician_From_Any"),
            avg("Total_Patient_From_Any").alias("Average_Total_Patient_From_Any"),
            sd("Total_Patient_From_Any").alias("SD_Total_Patient_From_Any"),
            avg("Total_Physician_From_SameSpecialty").alias("Average_Total_Physician_From_SameSpecialty"),
            sd("Total_Physician_From_SameSpecialty").alias("SD_Total_Physician_From_SameSpecialty"),
            avg("Total_Patient_From_SameSpecialty").alias("Average_Total_Patient_From_SameSpecialty"),
            sd("Total_Patient_From_SameSpecialty").alias("SD_Total_Patient_From_SameSpecialty"),
            avg("Total_Physician_From_Primarycare").alias("Average_Total_Physician_From_Primarycare"),
            sd("Total_Physician_From_Primarycare").alias("SD_Total_Physician_From_Primarycare"),
            avg("Total_Patient_From_Primarycare").alias("Average_Total_Patient_From_Primarycare"),
            sd("Total_Patient_From_Primarycare").alias("SD_Total_Patient_From_Primarycare")
        )

        return pdda_agged.filter(col("geo_value").isNotNull())
