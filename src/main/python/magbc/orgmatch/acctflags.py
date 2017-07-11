from smv import *
from pyspark.sql.functions import *

#from magbc.tsdm import acnt
import inputdata
import hcosccnmatch
from matcher import *
#from magbc.tsdm.orgmatch.matcher import *


class HcosAccount(SmvModule):
    """Comprehensive list of hcos accounts """

    def requiresDS(self):
        return [
            inputdata.AllAccounts,
            inputdata.ZipMaster
        ]

    def run(self, i):
        acc = i[inputdata.AllAccounts]
        zipM = i[inputdata.ZipMaster].select('zip', 'hsanum', 'hrrnum')

        hcos = acc.select(
            col('lvl1_org_id').alias('id'),
            col('lvl1_org_nm').alias('name'),
            col('lvl1_physical_addr_1').alias('address'),
            col('lvl1_physical_city').alias('city'),
            col('lvl1_physical_state').alias('state'),
            col('lvl1_physical_zip').alias('zip')
        ).smvJoinByKey(zipM, ['zip'], 'inner')

        #res = acc.smvJoinByKey(zipM, ['zip'], 'leftouter')

        return hcos

class MatchCoc(SmvModule):
    """ Match Hcos id's to Coc"""

    def requiresDS(self):
        return [
            HcosAccount,
            inputdata.CommissionOnCancer,
            inputdata.ZipMaster
        ]

    def run(self, i):
        hcos = i[HcosAccount]
        coc = i[inputdata.CommissionOnCancer]
        zipM = i[inputdata.ZipMaster].select('zip', 'hsanum', 'hrrnum')

        cocdf = coc.select(
            smvHashKey(col('Name')).alias('id'),
            col('Name').alias('name'),
            col('Type').alias('type'),
            'address',
            'city',
            'state',
            'zip'
        ).smvJoinByKey(zipM, ['zip'], 'leftouter')

        rawMatched = fullLevelMatch(hcos, cocdf)

        matched = hcos.smvJoinByKey(rawMatched.smvRenameField(('_id', 'right_id')), ['id'], 'inner'
        ).smvJoinByKey(cocdf.smvRenameField(('id', 'right_id')), ['right_id'], 'inner'
        ).smvSelectPlus(
            coalesce(col('L5_HRR_Name_Value'), lit(0.0)).alias('s1'),
            coalesce(col('L2_Address_Value'), lit(0.0)).alias('s2')
        ).smvDedupByKeyWithOrder('id')(
            col('MatchBitmap').desc(),
            col('s1').desc(),
            col('s2').desc()
        )

        return matched

class MatchOncCareModel(SmvModule):
    """Match hcos id's to OncCareModel id's"""

    def requiresDS(self):
        return [
            inputdata.OncCareModel,
            HcosAccount
        ]

    def run(self, i):
        onc = i[inputdata.OncCareModel]
        hcos = i[HcosAccount]

        oncdf = onc.select(
            col('Unique_ID').alias('id'),
            col('Organization_Name').alias('name'),
            col('City').alias('city'),
            col('Street_Address').alias('address'),
            col('State').alias('state')
        )

        rawMatched = noZipMatch(hcos, oncdf)

        matched = hcos.smvJoinByKey(rawMatched.smvRenameField(('_id', 'right_id')), ['id'], 'inner'
        ).smvJoinByKey(oncdf.smvRenameField(('id', 'right_id')), ['right_id'], 'inner'
        ).smvSelectPlus(
            coalesce(col('L3_State_Name_Value'), lit(0.0)).alias('s1'),
            coalesce(col('L1_AddressOnly_Value'), lit(0.0)).alias('s2')
        ).smvDedupByKeyWithOrder('id')(
            col('MatchBitmap').desc(),
            col('s1').desc(),
            col('s2').desc()
        )

        return matched

class MatchNccn(SmvModule):
    """Match hcos to nccn id's"""

    def requiresDS(self):
        return [
            HcosAccount,
            inputdata.NatCompCancerNetwork
        ]

    def run(self, i):
        nat = i[inputdata.NatCompCancerNetwork]
        hcos = i[HcosAccount]

        natdf = nat.select(
            'id',
            col('Name').alias('name'),
            col('City').alias('city'),
            col('State').alias('state')
        )

        rawMatched = noZipNoAddressMatch(hcos, natdf)

        matched = hcos.smvJoinByKey(rawMatched.smvRenameField(('_id', 'right_id')), ['id'], 'inner'
        ).smvJoinByKey(natdf.smvRenameField(('id', 'right_id')), ['right_id'], 'inner'
        ).smvSelectPlus(
            coalesce(col('L1_City_Name_L_Value'), lit(0.0)).alias('s1'),
            coalesce(col('L3_City_Name_N2_Value'), lit(0.0)).alias('s2')
        ).smvDedupByKeyWithOrder('id')(
            col('MatchBitmap').desc(),
            col('s1').desc(),
            col('s2').desc()
        )

        return matched


class MatchNcid(SmvModule):
    """ Match hcos to nci"""

    def requiresDS(self):
        return [
            HcosAccount,
            inputdata.NciDesignated,
            inputdata.ZipMaster
        ]

    def run(self, i):
        nci = i[inputdata.NciDesignated]
        hcos = i[HcosAccount]
        zipM = i[inputdata.ZipMaster].select('zip', 'hsanum', 'hrrnum')

        ncidf = nci.select(
            'id',
            col('name'),
            col('city'),
            col('address'),
            when(length(col('zip')) == 4, concat(lit('0'), col('zip'))
            ).otherwise(col('zip').substr(1, 5)).alias('zip')
        ).smvJoinByKey(zipM, ['zip'], 'leftouter')

        rawMatched = fullLevelMatch(hcos, ncidf)

        matched = hcos.smvJoinByKey(rawMatched.smvRenameField(('_id', 'right_id')), ['id'], 'inner'
        ).smvJoinByKey(ncidf.smvRenameField(('id', 'right_id')), ['right_id'], 'inner'
        ).smvSelectPlus(
            coalesce(col('L5_HRR_Name_Value'), lit(0.0)).alias('s1'),
            coalesce(col('L2_Address_Value'), lit(0.0)).alias('s2')
        ).smvDedupByKeyWithOrder('id')(
            col('MatchBitmap').desc(),
            col('s1').desc(),
            col('s2').desc()
        )

        return matched

class AcctFlags(SmvModule, SmvOutput):
    """ Combine matching results with flags for each group"""

    def requiresDS(self):
        return [
            HcosAccount,
            MatchCoc,
            MatchOncCareModel,
            MatchNcid,
            MatchNccn
        ]

    def run(self, i):
        hcos = i[HcosAccount].select('id', 'zip')
        onc = i[MatchOncCareModel].select('id', lit(1).alias('isOncCareModel'))
        nci = i[MatchNcid].select('id', lit(1).alias('isNCICancerCenter'))
        nccn = i[MatchNccn].select('id', lit(1).alias('isNatCompCancerNetwork'))
        coc = i[MatchCoc].select('id', lit(1).alias('isCommissionOnCancer'))

        return hcos.smvJoinMultipleByKey(['id'], 'leftouter'
        ).joinWith(coc, '_1'
        ).joinWith(onc, '_2'
        ).joinWith(nccn, '_3'
        ).joinWith(nci, '_4'
        ).doJoin().smvRenameField(
            ('id', 'ORGANIZATION_ID')
        )
