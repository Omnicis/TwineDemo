from smv import *
from pyspark.sql.functions import *

############## compare data #############
class GroupPracticeCQoC(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_2014_Group_Practice_Public_Reporting_-_Clinical_Quality_Of_Care.csv"

class GroupPracticePE(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_2014_Group_Practice_Public_Reporting_-_Patient_Experience.csv"

class PhysicianCompare(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_National_Downloadable_File.csv"

class PhysicianCQoC(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_2014_Individual_EP_Public_Reporting_-_Clinical_Quality_Of_Care.csv"

################# county health rank data #############
class Uninsured(SmvCsvFile):
    """percent uninsured at county_fips level"""
    def path(self):
        return "02_county_health_rank/20151029_Uninsured_Rates_by_County.csv"

class Underinsured(SmvCsvFile):
    """'geo_value'(state) level underinsured statistics"""
    def path(self):
        return "02_county_health_rank/under_insure_medicaid.csv"

################# dartmouth atlas data #################
class Capacity(SmvCsvFile):
    """hsa level hospital capacity stats"""
    def path(self):
        return "07_dartmouth_atlas/2012_hosp_resource_hsa.csv"

################# state cancer profile data #################
class IncStateBreastCancerProfile(SmvCsvFile):
    """breast cancer incidence information on fips level"""
    def path(self):
        return "10_state_cancer_profile/incd/incdbycounty.csv"

class ScreeningStateBreastCancerProfile(SmvCsvFile):
    """Breast cancer screening information on fips county level"""
    def path(self):
        return "10_state_cancer_profile/screening_and_risk/screening_risk_county.csv"

################# external project outputs #################
class CancerPatientCount(SmvCsvFile):
    """ demographic and disease statistics on geo_value(country, county, hrr or state) level"""
    def path(self):
        return "mag_external_dependency/CmsGeo.csv"

class CmsHCMeasure(SmvCsvFile):
    """ provider_id level hospital information"""
    def path(self):
        return "mag_external_dependency/CmsHCMeasure.csv"

class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"

