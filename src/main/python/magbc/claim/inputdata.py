from smv import *

#########Claim-Line level data from 155 patients#########
class Claims(SmvCsvFile):
    def path(self):
        return "twine_demo/claims.csv"

#########Information for target products#########
class ProductMaster(SmvCsvFile):
    def path(self):
        return "twine_demo/product_master.csv"
