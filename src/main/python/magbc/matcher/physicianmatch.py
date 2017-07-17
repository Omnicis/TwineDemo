from smv import *
from smv.matcher import *
from pyspark.sql.functions import *
from smv.functions import *
from magbc.core import *
import inputdata

class PhysicianMasterDF(SmvModule):
    def requiresDS(self):
        return [inputdata.PhysicianMaster]

    def run(self, i):
        df = i[inputdata.PhysicianMaster]
        return df

class NppesPhysicians(SmvModule):
    """
    NPPES Physician List
    """
    def requiresDS(self):
        return [inputdata.PhysicianMaster]

    def run(self, i):
        nppes = i[inputdata.PhysicianMaster]

        nppesPhyn = nppes.select(
            "NPI",
            "Provider_First_Name",
            "Provider_Last_Name_Legal_Name",
            "Provider_First_Line_Business_Practice_Location_Address",
            "Provider_Business_Practice_Location_Address_Postal_Code"
            ).filter(
            length(col("Provider_Business_Practice_Location_Address_Postal_Code")) >= 5
            ).withColumn(
            "Provider_Zip5", 
            col("Provider_Business_Practice_Location_Address_Postal_Code").substr(1,5)
            ).withColumn(
            "Provider_FullName", 
            smvStrCat(col("Provider_Last_Name_Legal_Name"), col("Provider_First_Name"))
            ).withColumn(
            "Provider_AddressID", 
            addressId(col("Provider_Zip5"), col("Provider_First_Line_Business_Practice_Location_Address"))
            )
            
        return nppesPhyn


class OpenPaymentPhysicians(SmvModule):
    """
    Open Payment Physician List
    """
    def requiresDS(self):
        return [inputdata.OpenPayment]

    def run(self, i):
        op = i[inputdata.OpenPayment]

        opPhyn = op.filter(
            col("Physician_Profile_ID").isNotNull()
            ).smvDedupByKey("Physician_Profile_ID"
            ).select(
            "Physician_Profile_ID",
            "Physician_First_Name",
            "Physician_Last_Name",
            "Recipient_Primary_Business_Street_Address_Line1",
            "Recipient_Zip_Code"
            ).filter(
            length(col("Recipient_Zip_Code")) >= 5
            ).withColumn(
            "Physician_Zip5", 
            col("Recipient_Zip_Code").substr(1,5)
            ).withColumn(
            "Physician_FullName", 
            smvStrCat(col("Physician_Last_Name"), col("Physician_First_Name"))
            ).withColumn(
            "Physician_AddressID", 
            addressId(col("Physician_Zip5"), col("Recipient_Primary_Business_Street_Address_Line1"))
            )

        return opPhyn


class PhysicianMatch(SmvModule, SmvOutput):
    """
    match physicians from OpenPayment to Nppes 
    """
    def requiresDS(self):
        return[OpenPaymentPhysicians, NppesPhysicians]

    def run(self, i):
        s1 = i[OpenPaymentPhysicians]
        s2 = i[NppesPhysicians]

        result = SmvEntityMatcher('Physician_Profile_ID', 'NPI',
        ExactMatchPreFilter(
            "AddressId_FullName_Match", 
            concat(col("Provider_AddressID"), col("Provider_FullName")) == \
            concat(col("Physician_AddressID"),col("Physician_FullName"))
            ),
        GroupCondition(
            soundex(col("Provider_Last_Name_Legal_Name")) == soundex(col("Physician_Last_Name"))
            ),
        [
          FuzzyLogic("AddressId_FuzzyFullName_Match",
            col("Provider_AddressID") == col("Physician_AddressID"),
            fullnamescore(
            jaroWinkler(col("Provider_Last_Name_Legal_Name"), col("Physician_Last_Name")), 
            jaroWinkler(col("Provider_First_Name"), col("Physician_First_Name"))), 
            0.9),
          ExactLogic("FullName_Zip_Match", 
            concat(col("Provider_FullName"), col("Provider_Zip5")) == \
            concat(col("Physician_FullName"), col("Physician_Zip5")))
        ]
        ).doMatch(s1, s2, False)

        return result


