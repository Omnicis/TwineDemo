from smv import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import inputdata
import etl


# class Prodts(SmvModule):
#     def requiresDS(self):
#         return [inputdata.Prodts]
#
#     def run(self, i):
#         df = i[inputdata.Prodts]
#         return df

class ForLot(SmvModule):
    def requiresDS(self):
        return [
            inputdata.Claims,
            etl.InscopeClaims,
            inputdata.ProductMaster
        ]

    def run(self, i):
        df = i[inputdata.Claims]
        prodts = i[inputdata.ProductMaster]
        claims = i[etl.InscopeClaims]

        #prodclaims =  df.smvJoinByKey(prodts, ['prodt'], 'leftouter')
        prodclaims = claims

        df2 = prodclaims.groupBy('ptnt_gid', 'dt').agg(
            max(when(col('prodt') == 'J9306', lit(1)).otherwise(lit(0))).alias('PER'),
            max(when(col('prodt') == 'J9354', lit(1)).otherwise(lit(0))).alias('KAD'),
            max(when(col('prodt') == 'J9355', lit(1)).otherwise(lit(0))).alias('HER'),
            #max(when(col('prodt').isin(['19120', '19125', '19126']), lit(1)).otherwise(lit(0))).alias('isExcision'),
            max(when(col('procType') == 'Surgery', lit(1)).otherwise(lit(0))).alias('isSurgery'),
            max(when(col('targetType') == 'Drug', lit(1)).otherwise(lit(0))).alias('isTreatment')
        )

        firsts = df2.groupBy('ptnt_gid').agg(
            min(when(col('isSurgery') == 1, col('dt'))).alias('firstSurgDt'),
            #min(when(col('isExcision') == 1, col('dt'))).alias('firstExDt'),
            min(when(col('isTreatment') == 1, col('dt'))).alias('firstTreatmentDt')
        )
        #
        #
        # df2 = df.groupBy('ptnt_gid', 'dt').agg(
        #     max(when(col('prodt') == 'J9306', lit(1)).otherwise(lit(0))).alias('PER'),
        #     max(when(col('prodt') == 'J9354', lit(1)).otherwise(lit(0))).alias('KAD'),
        #     max(when(col('prodt') == 'J9355', lit(1)).otherwise(lit(0))).alias('HER'),
        #     #max(when(col('prodt').isin(['19120', '19125', '19126']), lit(1)).otherwise(lit(0))).alias('isExcision'),
        #     max(when(col('prodt').isin(surglist), lit(1)).otherwise(lit(0))).alias('isSurgery'),
        #     max(when(col('prodt').isin(druglist), lit(1)).otherwise(lit(0))).alias('isTreatment')
        # )
        #
        # firsts = df2.groupBy('ptnt_gid').agg(
        #     min(when(col('isSurgery') == 1, col('dt'))).alias('firstSurgDt'),
        #     #min(when(col('isExcision') == 1, col('dt'))).alias('firstExDt'),
        #     min(when(col('isTreatment') == 1, col('dt'))).alias('firstTreatmentDt')
        # )
        #
        # win = Window.partitionBy(col('ptnt_gid')).orderBy(col('dt'))
        #
        # withGap = df2.withColumn('gap',
        #     coalesce(datediff(col('dt'), lag('dt').over(win)), lit(0))
        # )
        #
        # return withGap.smvJoinByKey(firsts, ['ptnt_gid'], 'inner')

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
