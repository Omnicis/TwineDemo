from smv import *


class PhysicianMaster(SmvCsvFile):
    """ Physician master data by NPPES"""
    def path(self):
        return "twine_demo/npidata_20050523-20160710.csv"

class OpenPayment(SmvCsvFile):
    """ Pharmaceutical payments to physicians """
    def path(self):
        return "twine_demo/OP_DTL_GNRL_PGYR2015_P06302016.csv"