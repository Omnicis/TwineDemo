from smv import *
from pyspark.sql.functions import *

import ccndriver
from matcher import *
import inputdata
#from magbc.core import *
import re

class TopAccounts(SmvModule):
    def requiresDS(self):
        return [
            inputdata.TopAccts,
            inputdata.AllAccounts
        ]

    def run(self, i):
        acc = i[inputdata.AllAccounts]
        ta = i[inputdata.TopAccts]

        top = ta.smvJoinByKey(acc, ['lvl1_org_id'], 'inner')

        topacc = top.select(
            col('lvl1_org_id').alias('id'),
            col('lvl1_org_nm').alias('name'),
            col('lvl1_physical_addr_1').alias('address'),
            col('lvl1_physical_state').alias('state'),
            col('lvl1_physical_city').alias('city'),
            col('lvl1_physical_zip').substr(1,5).alias('zip')
        )

        return topacc

class HcosCcnMatch(SmvModule, SmvOutput):
    """Match Hcos id's to ccn"""

    def requiresDS(self):
        return [
            TopAccounts,
            inputdata.AllAccounts,
            ccndriver.CcnDriver,
            inputdata.ZipMaster
        ]

    def run(self, i):
        acc = i[inputdata.AllAccounts]
        ta = i[TopAccounts]

        ccn = i[ccndriver.CcnDriver].smvRenameField(('CCN', 'id'))
        zipM = i[inputdata.ZipMaster].select(
            'zip',
            col('hsanum'),
            col('hrrnum')
        )

        hcos = ta.smvJoinByKey(zipM, ['zip'], 'leftouter')

        rawMatched = fullLevelMatch(ccn, hcos).smvRenameField(('_id', 'Org_ID'))



        matched = rawMatched.smvSelectPlus(
            coalesce(col('L5_HRR_Name_Value'), lit(0)).alias('s1'),
            coalesce(col('L2_Address_Value'), lit(0)).alias('s2')
        ).smvDedupByKeyWithOrder('Org_ID')(
            col('MatchBitmap').desc(),
            col('s1').desc(),
            col('s2').desc()
        ).smvRenameField(
            ('id', 'CCN')
        ).smvDedupByKeyWithOrder('CCN')(
            col('MatchBitmap').desc(),
            col('s2').desc(),
            col('s1').desc()
        )

        res = matched
        return matched

class HcosPacMatch(SmvModule, SmvOutput):
    """Match hcos to pac id's"""

    def requiresDS(self):
        return [
            inputdata.PhysicianCompare,
            TopAccounts,
            inputdata.AllAccounts
        ]

    def run(self, i):
        gp = i[inputdata.PhysicianCompare]
        # acc = i[inputdata.AllAccounts].smvRenameField(
        #     ('lvl1_org_id', 'id'),
        #     ('lvl1_org_nm', 'name'),
        #     ('lvl1_physical_addr_1', 'address'),
        #     ('lvl1_physical_state', 'state'),
        #     ('lvl1_physical_city', 'city'),
        #     ('lvl1_physical_zip', 'zip')
        # )

        acc = i[TopAccounts]

        gpdf = gp.smvRenameField(
            ('Group_Practice_PAC_ID', 'id'),
            ('Organization_legal_name', 'name'),
            ('State', 'state')
        )

        rawMatched = nameAndStateMatch(gpdf.smvDedupByKey('id'), acc
        ).smvRenameField(('_id', 'Org_ID'))

        matched = rawMatched.smvDedupByKeyWithOrder('Org_ID')(col('L1_Name_Value').desc()
        ).smvRenameField(('id', 'Group_Practice_PAC_ID'))

        return matched #
