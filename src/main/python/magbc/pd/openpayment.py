from smv import *
from pyspark.sql.functions import *
import inputdata
from magbc.core import targetDrugList


class OpenPaymentPhynStats(SmvModule, SmvOutput):
    """
    Aggregate target drug OpenPayment Amount to physician level
    """
    def requiresDS(self):
        return [inputdata.OpenPayment]

    def run(self, i):
        op = i[inputdata.OpenPayment]

        opagg = op.withColumn("drug", 
            upper(col("Name_of_Associated_Covered_Drug_or_Biological1"))
            ).filter(
            upper(col("drug")).isin(targetDrugList)
            ).groupBy("Physician_Profile_ID"
            ).pivot("drug"
            ).agg(sum("Total_Amount_of_Payment_USDollars")
            )

        return opagg.smvRenameField(*[(x, "Total_Amount_of_Payment_" + x) for x in targetDrugList])

