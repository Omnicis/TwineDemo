from smv import *
from magbc.claim import etl
from magbc.pd import openpayment
from magbc.matcher import physicianmatch

#########Information for Physicians#########
class PhynProfile(SmvCsvFile):
    def path(self):
        return "twine_demo/physician_profile.csv"

#########Output from previous stage#########
PhysicianLevelStats = SmvModuleLink(etl.PhysicianLevelStats)

OpenPaymentPhynStats = SmvModuleLink(openpayment.OpenPaymentPhynStats)

PhysicianMatch = SmvModuleLink(physicianmatch.PhysicianMatch)