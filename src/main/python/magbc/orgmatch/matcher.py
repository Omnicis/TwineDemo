from smv import *
from smv.matcher import *
from smv.functions import *
from pyspark.sql.functions import *
import re


stopWords = [
    "THE",
    "OF",
    "AND",
    "AT",
    "INC",
    "LLC"
]

highFreqWords = {
    "HOSPITAL"    : "HO",
    "HOSP"        : "HO",
    "CENTER"      : "CT",
    "CENTERS"     : "CT",
    "CTR"         : "CT",
    "MEDICAL"     : "MD",
    "MED"         : "MD",
    "INSTITUTE"   : "INST",
    "HEALTHCARE"  : "HE",
    "HEALTH"      : "HE",
    "SYSTEM"      : "S",
    "CARE"        : "C",
    "UNIVERSITY"  : "U",
    "UNIV"        : "U",
    "RESEARCH"    : "RES",
    "SAINT"       : "ST",
    "CLINIC"      : "CL",
    "ONCOLOGY"    : "ONC",
    "GROUP"       : "G",
    "SPECIALISTS" : "SP"
}

addHighFreq = {
    "LANE"        : "LN",
    "ROAD"        : "RD",
    "AVENUE"      : "AV",
    "AVE"         : "AV",
    "STREET"      : "ST",
    "STR"         : "ST",
    "SOUTH"       : "S",
    "NORTH"       : "N",
    "EAST"        : "E",
    "WEST"        : "W",
    "BOULEVARD"   : "BL",
    "BOULVARD"    : "BL",
    "BLVD"        : "BL",
    "DRIVE"       : "DR",
    "PLACE"       : "PL",
    "WAY"         : "WA",
    "PARKWAY"     : "PW",
    "PKWY"        : "PW",
    "HIGHWAY"     : "HW",
    "HWY"         : "HW",
    "ONE"         : "1",
    "TWO"         : "2",
    "THREE"       : "3",
    "FOUR"        : "4",
    "FIVE"        : "5",
    "SIX"         : "6",
    "SEVEN"       : "7",
    "EIGHT"       : "8",
    "NINE"        : "9",
    "TEN"         : "10"
}

def fn(x):
    if (x in highFreqWords):
        return highFreqWords[x]
    else:
        return x

def normName(name):

    def norm(s):
        highFreqWord = highFreqWords
        stopWord = stopWords
        def fn(x):
            if (x in highFreqWord):
                return highFreqWord[x]
            else:
                return x
        if (s is None):
            return None
        else:
            u = s.upper()
            n = re.sub(r"[^0-9a-zA-Z]""", " ", u
            ).split()
            f = filter(lambda x: not x in stopWord, n)
            return ' '.join(map(fn, f))

    g = udf(norm)
    return g(name)


def normAddress(addr):
    def norm(s):
        highFreqWord = addHighFreq
        stopWord = stopWords
        def fn(x):
            if (x in highFreqWord):
                return highFreqWord[x]
            else:
                return x
        if (s is None):
            return None
        else:
            u = s.upper().split(',')[0]
            n = re.sub(r"[^0-9a-zA-Z]""", " ", u
            ).split()
            f = filter(lambda x: not x in stopWord, n)
            return ' '.join(map(fn, f))

    g = udf(norm)
    return g(addr)

def addressId(zip5, addr):
    def id(s1, s2):
        if (s1 is None or s2 is None):
            return None
        else:
            m = re.match('(\d+)', s2)
            if (m is not None):
                return s1 + m.group()
            else:
                return None

    g = udf(id)
    return g(zip5, addr)

def fullLevelMatch(df1, df2):

    def dfNorm(df):
        return df.select(
            'id',
            addressId('zip', 'address').alias('address_id'),
            normName('name').alias('normName'),
            'zip',
            normAddress('address').alias('normAddress'),
            upper(col('city')).alias('city'),
            'hsanum',
            'hrrnum'
        )

    ndf1 = dfNorm(df1)
    ndf2 = dfNorm(df2).smvPrefixFieldNames('_')

    mdf = SmvEntityMatcher('id', '_id',
        None,
        GroupCondition(col('hrrnum') == col('_hrrnum')),
        [
            ExactLogic('L1_AddressId', (col('address_id').isNotNull()) & (col('address_id') == col('_address_id'))),
            FuzzyLogic('L2_Address', col('zip') == col('_zip'), normlevenshtein(col('normAddress'), col('_normAddress')), 0.6),
            FuzzyLogic('L3_City_Name', col('city') == col('_city'), normlevenshtein(col('normName'), col('_normName')), 0.5),
            FuzzyLogic('L7_Zip_Name', col('zip') == col('_zip'), normlevenshtein(col('normName'), col('_normName')), 0.4),
            FuzzyLogic('L6_City_Address', col('city') == col('_city'), normlevenshtein(col('normAddress'), col('_normAddress')), 0.65),
            FuzzyLogic('L4_HSA_Name', col('hsanum') == col('_hsanum'), normlevenshtein(col('normName'), col('_normName')), 0.6),
            FuzzyLogic('L5_HRR_Name', lit(True), normlevenshtein(col('normName'), col('_normName')), 0.6)

        ]
    ).doMatch(ndf1, ndf2, False)

    return mdf

def noZipMatch(df1, df2):

    def dfNorm(df):
        return df.select(
            'id',
            normName('name').alias('normName'),
            normAddress('address').alias('normAddress'),
            upper(col('city')).alias('city'),
            upper(col('state')).alias('state')
        )

    ndf1 = dfNorm(df1)
    ndf2 = dfNorm(df2).smvPrefixFieldNames('_')

    mdf = SmvEntityMatcher('id', '_id',
        #NoOpPreFilter,
        None,
        GroupCondition(col('state') == col('_state')),
        [
            FuzzyLogic('L1_AddressOnly', col('city') == col('_city'), normlevenshtein(col('normAddress'), col('_normAddress')), 0.8),
            FuzzyLogic('L2_City_Name', col('city') == col('_city'), normlevenshtein(col('normName'), col('_normName')), 0.6),
            FuzzyLogic('L3_State_Name', lit(True), normlevenshtein(col('normName'), col('_normName')), 0.8)
        ]
    ).doMatch(ndf1, ndf2, False)

    return mdf

def noZipNoAddressMatch(df1, df2):

    def dfNorm(df):
        return df.select(
            'id',
            normName('name').alias('normName'),
            upper(col('city')).alias('city'),
            upper(col('state')).alias('state')
        )

    ndf1 = dfNorm(df1)
    ndf2 = dfNorm(df2).smvPrefixFieldNames('_')

    mdf = SmvEntityMatcher('id', '_id',
        None,
        GroupCondition(col('state') == col('_state')),
        [
            FuzzyLogic('L1_City_Name_L', col('city') == col('_city'), normlevenshtein(col('normName'), col('_normName')), 0.4),
            FuzzyLogic('L2_State_Name_L', lit(True), normlevenshtein(col('normName'), col('_normName')), 0.6),
            FuzzyLogic('L3_City_Name_N2', col('city') == col('_city'), nGram2(col('normName'), col('_normName')), 0.4),
            FuzzyLogic('L4_State_Name_N2', lit(True), normlevenshtein(col('normName'), col('_normName')), 0.6)
        ]
    ).doMatch(ndf1, ndf2, False)

    return mdf

def nameAndStateMatch(df1, df2):

    def dfNorm(df):
        return df.select(
            'id',
            normName('name').alias('normName'),
            upper(col('state')).alias('state')
        )

    ndf1 = dfNorm(df1)
    ndf2 = dfNorm(df2).smvPrefixFieldNames('_')

    mdf = SmvEntityMatcher('id', '_id',
        None,
        GroupCondition(col('state') == col('_state')),
        [
            FuzzyLogic('L1_Name', lit(True), normlevenshtein(col('normName'), col('_normName')), 0.6) #.85
        ]
    ).doMatch(ndf1, ndf2, False)

    return mdf


#def match(df1, df2, matcher, orders):
#    dedupers = [coalesce(col(s), lit(0)).desc() for s in orders]
#
#    rawMatched = matcher(df1, df2)
#    matched = df1.smvJoinByKey(rawMatched, ['id'], 'inner'
#    ).smvJoinByKey(df2, ['_id'], 'inner'
#    ).smvDedupByKeyWithOrder('id')(
#        col('MatchBitmap').desc(),
#        *dedupers
#    )
#
#    return matched
