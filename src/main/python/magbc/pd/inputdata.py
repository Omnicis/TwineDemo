from smv import *


class OpenPayment(SmvCsvFile):
    """ Pharmaceutical payments to physicians """
    def path(self):
        return "15_open_payment/2015/OP_DTL_GNRL_PGYR2015_P06302016.csv"