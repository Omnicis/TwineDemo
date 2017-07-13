from smv import *


class PhysicianMaster(SmvCsvFile):
    """ Physician master data by NPPES"""
    def path(self):
        return "04_npi_master/201607/npidata_20050523-20160710.csv"

class OpenPayment(SmvCsvFile):
    """ Pharmaceutical payments to physicians """
    def path(self):
        return "15_open_payment/2015/OP_DTL_GNRL_PGYR2015_P06302016.csv"