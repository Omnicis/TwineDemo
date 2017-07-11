from smv import *
from pyspark.sql.functions import *
from magbc.etl import claim
from magbc.sha import byhsa

class KolMaster(SmvCsvFile):
    """ NPI level aggregates information"""
    def path(self):
        return "mag_external_dependency/KolMaster.csv"

ZipHierMap = SmvModuleLink(byhsa.ZipHierMap)

class PhysicianCompare(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_National_Downloadable_File.csv"
