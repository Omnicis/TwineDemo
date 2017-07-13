from smv import *
from magbc.claim import etl

#########zip master#########
class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"
