from smv import *
from magbc.phys import physician

#########zip master#########
class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "twine_demo/ZipMaster.csv"

#########Output from previous stage#########
AllPhynStats = SmvModuleLink(physician.AllPhynStats)
