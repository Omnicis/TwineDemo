from smv import *
from magbc.sha import byhsa
from magbc.pdda import pdda
from magbc.hsa import hsamaster
from magbc.etl import claim
from magbc.hsa import compare
from magbc.orgmatch import acctflags, hcosccnmatch

ZipHierMap = SmvModuleLink(byhsa.ZipHierMap)
LotByCohortHsa = SmvModuleLink(byhsa.LotByCohortHsa)
TestStatsGeoLevel = SmvModuleLink(byhsa.TestStatsGeoLevel)
PddaStatsGeoLevel = SmvModuleLink(pdda.PddaStatsGeoLevel)
HsaHrrMaster = SmvModuleLink(hsamaster.HsaHrrMaster)
PhyByHsa = SmvModuleLink(byhsa.PhyByHsa)
GeoStdOfCare =SmvModuleLink(byhsa.GeoStdOfCare)
AllPhyns = SmvModuleLink(claim.AllPhyns)
LoT = SmvModuleLink(claim.PtntLoTRaw)
HospCompare = SmvModuleLink(compare.HospCompare)


class KolMaster(SmvCsvFile):
    """ NPI level aggregates information"""
    def path(self):
        return "mag_external_dependency/KolMaster.csv"

class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"

HcosCcnMatch = SmvModuleLink(hcosccnmatch.HcosCcnMatch)
AcctFlags = SmvModuleLink(acctflags.AcctFlags)
