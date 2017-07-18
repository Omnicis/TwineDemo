import abc
#from com.gene.gsonar.lung.core import *
from relib.RE.lotre import LoTRule, InitLoTRule, LoTPostProc


class IR(InitLoTRule):
    def stateSchema(self):
        return [
            ('LOT', 'double'),
            ('prev_lot', 'double'),
            ('prev_dt', 'timestamp'),
            ('progression_reason', 'string')
        ]

    def initState(self, r):
        s = {}
        if (pydateDiff(r.firstSurgDt, r.dt) is None):
            s['progression_reason'] = 'Patient not Initialized'
            s['LOT'] = 0.0
            s['prev_lot'] = 0.0
        elif (pydateDiff(r.firstSurgDt, r.dt) <= 0):
            s['LOT'] = 1.0
            s['prev_lot'] = 1.0
            s['progression_reason'] = 'M-Initialization of the Patient - Starting LOT'
        elif (pydateDiff(r.firstSurgDt, r.dt) > 0):
            s['LOT'] = 0.1
            s['prev_lot'] = 0.1
            s['progression_reason'] = 'Initialization of the Patient - Starting LOT'

        s['prev_dt'] = r.dt
        return s

class LoTRuleWithDefault(LoTRule):
    """Base class for all the Line of Therapy Rules with default action"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def lotUpdate(self, r, s):
        """Lot update"""

    def update(self, r, s):
        s['prev_dt'] = r.dt
        s['progression_reason'] = self.reason()
        self.lotUpdate(r, s)

class HER(LoTRuleWithDefault):
    def reason(self):
        return 'Taking Herceptin'

    def condition(self, r, s):
        return (
            s['LOT'] == 0.1 and
            r.HER == 1
        )

    def lotUpdate(self, r, s):
        s['LOT'] = 1.0
        #s['prev_lot'] = 1.0

class PER(LoTRuleWithDefault):
    def reason(self):
        return 'Taking perjeta'

    def condition(self, r, s):
        return (
            s['LOT'] >= 1.0 and
            r.PER == 1.0
        )

    def lotUpdate(self, r, s):
        s['LOT'] = s['prev_lot'] + 1

class NoOtherRule(LoTRuleWithDefault):
    def reason(self):
        return "No Other rule"

    def condition(self, r, s):
        return 1

    def lotUpdate(self, r, s):
        s['LOT'] = s['prev_lot']


class AlwaysDo(LoTPostProc):
    def update(self, r, s):
        s['prev_lot'] = s['LOT']

def pydateDiff(end, start):
    return None if (end is None or start is None) else (end - start).days
