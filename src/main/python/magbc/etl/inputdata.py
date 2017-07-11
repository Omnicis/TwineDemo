from smv import *
from pyspark.sql.functions import *
from magbc.core import HiveWithSampling, GSonarHiveTable

"""gSonar Layer 2"""
class PxDxRxONC(HiveWithSampling):
    """gSonar layer2 oncology claims"""
    def gSonarTableName(self):
        return "clm_sha_stg_pxdx_rx_onc"

    def sampling_key(self):
        return "ptnt_gid"


class PtntTumor(HiveWithSampling):
    """gSonar layer2 patient Uuniverse, tumor types = BREAST"""
    def gSonarTableName(self):
        return "clm_sha_stg_ptnt_tumor"

    def sampling_key(self):
        return "ptnt_gid"

    def run(self, df):
        # Since smv's _reload may load another "PtntTumor" classobj so that super method
        # may complain that the "self" is not an instance of "PtntTumor", instead of using
        # super, we refer the super class's name
        # return super(PtntTumor, self).run(df).where(col("tumor_type") == "BREAST")
        return HiveWithSampling.run(self, df).where(col("tumor_type") == "BREAST")


class DrugMaster(GSonarHiveTable):
    """gSonar layer2 drug master, mkt_nm = ONC"""
    def gSonarTableName(self):
        # new vendor_gsonar db's original clm_sha_drug_dim is a view
        # and somehow can't read from Spark, so made a copy
        return "clm_sha_drug_dim_copy"

    def run(self, df):
        return df.where(df.mkt_nm == "ONC")


class PhynDim(GSonarHiveTable):
    """gSonar layer2 physician dimension"""
    def gSonarTableName(self):
        return "sha_phyn_dim"


class Affl(GSonarHiveTable):
    """gSonar layer2 physician affiliation"""
    def gSonarTableName(self):
        return "affl_ms_int_blended"

    def run(self, df):
        return df.where(df.fld_frc_cd == "HER2")


"""gSonar Layer 4"""
class PtntLoT(HiveWithSampling):
    """gSonar layer4 Patient Line of Therapy"""
    def gSonarTableName(self):
        return "ptnt_lot_her2_fact"

    def sampling_key(self):
        return "ptnt_gid"
