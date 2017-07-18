import abc

class LoTRule(object):
    """Base class for all the Line of Therapy Rules"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def reason(self):
        """Rule fire reason"""

    @abc.abstractmethod
    def condition(self, row, state):
        """Rule condition, takes a row, and a state, return a boolean"""

    @abc.abstractmethod
    def update(self, row, state):
        """Action triggered by condition. `state` will get updated, return None"""

class InitLoTRule(object):
    """Base class for initial rule for each group of data"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def stateSchema(self):
        """Specify Schema of the `state`"""

    @abc.abstractmethod
    def initState(self, row, stateSchema):
        """Init the state from the first row of each group, return a dictionary as
        the init state."""

class LoTPostProc(object):
    """Base class for Line of Therapy Post rull process. Only process on State"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def update(self, row, state):
        """State update, return None"""

class LoTRE(object):
    """Line of Therapy rule engine"""

    def __init__(self, initRule):
        self.initRule = initRule()
        self.rules = []
        self.postProcs = []

    def addRuleChain(self, ruleChain):
        """Add a chain of rules. Rule chain is a list of rules which will be fired
        sequentially in a set of if-else if-...-else block"""
        self.rules.append(map(lambda C: C(), ruleChain))
        return(self)

    def addPostProc(self, proc):
        """Add a Post Rule Process to update state"""
        self.postProcs.append(proc())
        return(self)

    def applyToDF(self, df, keys, orders):
        """Apply RE to a DF:
        E.g.
        re.applyToDF(df, ['id'], ['dt'])
        """

        def buildKeyFunc(keys):
            ks = ", ".join(map(lambda k: "r." + k, keys))
            return eval("lambda r: (" + ks + ")")

        keyFn = buildKeyFunc(keys)
        orderFn = buildKeyFunc(orders)

        def execRuleChain(row, state, rc):
            """Execute a chain of rules, simulate a chain of
            if-elif-elif-..."""
            for rule in rc:
                if (rule.condition(row, state)):
                    rule.update(row, state)
                    return

        def mergeRow(row, state, stateSchema):
            stateList = [state[f] for (f, t) in stateSchema]
            res = [v for v in row] + stateList
            return res

        def addSchema(s, stateS):
            return reduce(lambda s0, f: s0.add(f[0], f[1]), [s] + stateS)

        initRule = self.initRule
        rules = self.rules
        postProcs = self.postProcs

        def runRules(groupedRows):
            """Process the grouped data by firing rules and processes"""
            isFirst = True
            state = {}
            res = []
            for r in groupedRows:
                if(isFirst):
                     state = initRule.initState(r)
                     isFirst = False
                else:
                    for ruleChain in rules:
                        execRuleChain(r, state, ruleChain)

                for p in postProcs:
                    p.update(r, state)

                res.append(mergeRow(r, state, initRule.stateSchema()))

            return res

        res = df.rdd.groupBy(keyFn).flatMapValues(lambda rs: runRules(sorted(rs, key=orderFn))).values()
        schema = addSchema(df.schema, initRule.stateSchema())
        return df.sql_ctx.createDataFrame(res, schema)
