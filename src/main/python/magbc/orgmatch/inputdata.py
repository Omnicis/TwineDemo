from smv import *
from pyspark.sql.functions import *
from magbc.etl import claim

TopAccts = SmvModuleLink(claim.LotTopAccount)
AllAccounts = SmvModuleLink(claim.AllAccts)

class CmsHCMeasure(SmvCsvFile):
    """ provider_id level hospital information"""
    def path(self):
        return "mag_external_dependency/CmsHCMeasure.csv"

class HospHSA(SmvCsvFile):
    """ provider level hospital and location information"""
    def path(self):
        return "07_dartmouth_atlas/hosp_hsa_hrr_2012.csv"

    def run(self, df):
        return df.withColumn('CCN', lpad(col('provider'), 6, '0'))

class ZipMaster(SmvCsvFile):
    """zip level location information and pop stats"""
    def path(self):
        return "mag_external_dependency/ZipMaster.csv"

class PhysicianCompare(SmvCsvFile):
    def path(self):
        return "12_physician_compare/201607/Physician_Compare_National_Downloadable_File.csv"

class OncCareModel(SmvCsvFile):
    def path(self):
        return "99_cancer_others/Oncology_Care_Model.csv"

class NatCompCancerNetwork(SmvCsvFile):
    def path(self):
        return "99_cancer_others/nccn.csv"

class NciDesignated(SmvCsvFile):
    def path(self):
        return "99_cancer_others/Flag2.csv"

class CommissionOnCancer(SmvCsvFile):
    def path(self):
        return "99_cancer_others/Commission_on_Cancer_list.txt"

    def run(self, df):

        def getAddr(addr):
            def p(s):
                a = map(lambda x: x.strip(), s.split(','))
                return  ', '.join(a[:-2])
            p_udf = udf(p)
            return p_udf(addr)

        def getCity(addr):
            def p(s):
                a = map(lambda x: x.strip(), s.split(','))
                return a[-2]
            p_udf = udf(p)
            return p_udf(addr)

        def getState(addr):
            def p(s):
                a = map(lambda x: x.strip(), s.split(','))
                return a[-1].split(' ')[0]
            p_udf = udf(p)
            return p_udf(addr)

        def getZip(addr):
            def p(s):
                a = map(lambda x: x.strip(), s.split(','))
                return a[-1].split(' ')[1]
            p_udf = udf(p)
            return p_udf(addr)

        return df.smvSelectPlus(
            getAddr(col('FullAddress')).alias('address'),
            getCity(col('FullAddress')).alias('city'),
            getState(col('FullAddress')).alias('state'),
            getZip(col('FullAddress')).alias('zip'),
        ).drop('FullAddress')
