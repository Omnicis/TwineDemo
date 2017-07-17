from smv import *


class OpenPayment(SmvCsvFile):
    """ Pharmaceutical payments to physicians """
    def path(self):
        return "twine_demo/OP_DTL_GNRL_PGYR2015_P06302016.csv"