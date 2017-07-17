"""
Project level shared functions (Driver side functions only) and constants
"""
from pyspark.sql.functions import *
import re

from twinelib.hier import HierarchyUtils



def addressId(zip5, addr):
    def id(s1, s2):
        if (s1 is None or s2 is None):
            return None
        else:
            m = re.search('(\d+)', s2)
            if (m is not None):
                return s1 + m.group()
            else:
                return None

    g = udf(id)
    return g(zip5, addr)


def fullnamescore(fns, lns):
    def score (sc1, sc2): 
        if (sc1 is None or sc2 is None):
            return None
        else:
            return (sc1 + sc2)/2.0

    sf = udf(score)
    return sf(fns, lns)


targetDrugList = ["HERCEPTIN", "KADCYLA", "PERJETA"]