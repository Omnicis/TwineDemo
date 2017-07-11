from smv import *
from magbc.etl import claim
from pyspark.sql.functions import *
from magbc.core import *

AllPhyns = SmvModuleLink(claim.AllPhyns)
FilteredClaims = SmvModuleLink(claim.FilteredClaims)
LoT = SmvModuleLink(claim.PtntLoTRaw)

class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"
