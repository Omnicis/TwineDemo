from smv import *
from twinelib.matcher import MatcherUtils
import inputdata

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

        opPhyn = op.smvDedupByKey("Physician_Profile_ID"
            ).select(
            "Physician_Profile_ID",
            "Physician_First_Name",
            "Physician_Last_Name",
            "Recipient_Primary_Business_Street_Address_Line1",
            "Recipient_Zip_Code"
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

        matcherUtils = MatcherUtils(
            {"id": ["Physician_Profile_ID", "NPI"],
             "first_name": ["Physician_First_Name", "Provider_First_Name"],
             "last_name": ["Physician_Last_Name", "Provider_Last_Name_Legal_Name"],
             "address_first_line": ["Recipient_Primary_Business_Street_Address_Line1", "Provider_First_Line_Business_Practice_Location_Address"],
             "zip": ["Recipient_Zip_Code","Provider_Business_Practice_Location_Address_Postal_Code"]})


        return matcherUtils.physicianMatcher(s1,s2)


