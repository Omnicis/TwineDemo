import abc
#from com.gene.gsonar.lung.core import *
from relib.RE.lotre import LoTRule, InitLoTRule, LoTPostProc


class IR(InitLoTRule):
    """ Assign initial lots based on first surgery date"""
    def stateSchema(self):
        return [
            ('LOT', 'double'),
            ('prev_lot', 'double'),
            ('prev_dt', 'timestamp'),
            ('progression_reason', 'string')
        ]

    def initState(self, r):
        s = {}
        if (pydateDiff(r.firstSurgDt, r.dt) is None or pydateDiff(r.firstSurgDt, r.dt) > 0):
            s['LOT'] = 0.1
            s['prev_lot'] = 0.1
            s['progression_reason'] = 'Initialization of the Patient - Starting LOT'
        else:
            s['LOT'] = 1.0
            s['prev_lot'] = 1.0
            s['progression_reason'] = 'M-Initialization of the Patient - Starting LOT'

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
    """If early stage patient is taking herceptin on current date, set lot to 1.0 """
    def reason(self):
        return 'Taking Herceptin'

    def condition(self, r, s):
        return (
            s['LOT'] == 0.1 and
            r.HER == 1
        )

    def lotUpdate(self, r, s):
        s['LOT'] = 1.0


class PER(LoTRuleWithDefault):
    """ If later stage patient is taking perjeta, increase lot """
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
    """ Default if no previous rule is fired"""
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
