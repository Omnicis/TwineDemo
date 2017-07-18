from smv import *
from pyspark.sql.functions import *
import etl

class ForLot(SmvModule):
    def requiresDS(self):
        return [
            etl.InscopeClaims
        ]

    def run(self, i):
        prodclaims = i[etl.InscopeClaims]

        def dayAgg(column, code, colname):
            return max(when(col(column) == code, lit(1)).otherwise(lit(0))).alias(colname)

        df2 = prodclaims.groupBy('ptnt_gid', 'dt').agg(
            dayAgg('prodt_cd', 'PERJETA', 'PER'),
            dayAgg('prodt_cd', 'HERCEPTIN', 'HER'),
            dayAgg('procType', 'Surgery', 'isSurgery'),
            dayAgg('targetType', 'Drug', 'isTreatment')
        )

        firsts = df2.groupBy('ptnt_gid').agg(
            min(when(col('isSurgery') == 1, col('dt'))).alias('firstSurgDt'),
            min(when(col('isTreatment') == 1, col('dt'))).alias('firstTreatmentDt')
        )

        return df2.smvJoinByKey(firsts, ['ptnt_gid'], 'inner')


class Lot(SmvModule, SmvOutput):
    def requiresDS(self):
        return [ForLot]

    def run(self, i):
        from relib.RE.lotre import LoTRE
        import relib.rules as r

        df = i[ForLot]

        re=LoTRE(r.IR).addRuleChain(
            [
                r.HER,
                r.PER,
                r.NoOtherRule
            ]
        ).addPostProc(
            r.AlwaysDo
        )

        res=re.applyToDF(df, ['ptnt_gid'], ['dt'])

        return res
