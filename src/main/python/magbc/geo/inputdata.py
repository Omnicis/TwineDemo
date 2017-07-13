from smv import *
from magbc.claim import etl

#########zip master#########
class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"

#########Output from previous stage#########
InscopeClaims = SmvModuleLink(etl.InscopeClaims)

#########Information for Physicians#########
class PhynProfile(SmvCsvFile):
    def path(self):
        return "twine_demo/physician_profile.csv"