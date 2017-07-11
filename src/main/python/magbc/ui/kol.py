from smv import *
import inputdata
from hsamap import GeoDriver
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class NpiMaster(SmvModule):
    """NPI level master from CMS data
    """
    def requiresDS(self):
        return [inputdata.KolMaster, inputdata.ZipMaster]

    def run(self, i):
        df = i[inputdata.KolMaster]
        zip = i[inputdata.ZipMaster]

        return df.withColumn("zip", col("PECOS_Zip_Code").substr(1, 5)
          ).smvJoinByKey(zip, ["zip"], "leftouter")


class KolMasterForUi(SmvModule):
    """KoL with Ui specific vars"""
    def requiresDS(self):
        return [NpiMaster, GeoDriver]

    def run(self, i):
        df = i[NpiMaster]
        geo = i[GeoDriver]

        hrr_list = geo.where(geo.geo_type == 'hrr').select(
            geo.geo_value.alias('hrrnum')
        )

        inscope = df.where((df.Pub_Cnt_Total > 0) | (df.Trail_Cnt_Total > 0)).select(
            'NPI',
            concat(df.NPPES_Provider_Last_Name_Organization_Name, lit(', '), df.NPPES_Provider_First_Name).alias('kol_name'),
            'Medicare_Provider_Type',
            'Patient_Cnt_Total',
            'Patient_Cancer_Pct',
            'Pub_Cnt_Total',
            "Trail_Cnt_Total",
            "OpenPayment_Amt_Total",
            "Total_Physician_From_Any",
            "hsanum",
            "hrrnum"
        ).smvJoinByKey(hrr_list, ['hrrnum'], 'inner')

        all_vars = [
            'Patient_Cnt_Total',
            'Pub_Cnt_Total',
            'Trail_Cnt_Total',
            'OpenPayment_Amt_Total',
            'Total_Physician_From_Any'
        ]

        weights = [
            0.1,
            1,
            1,
            1,
            0.5
        ]

        var_scrs = reduce(
            lambda df, c: df.withColumn(c + "_pctrk", percent_rank().over(Window.orderBy(c))),
            all_vars,
            inscope
        )

        raw_scr_col = reduce(lambda x, y: x + y, [(col(c + "_pctrk") * lit(w)) for (c, w) in zip(all_vars, weights)])
        res = var_scrs\
            .withColumn('raw_scr', raw_scr_col)\
            .withColumn('full_scr', percent_rank().over(Window.orderBy('raw_scr')))\
            .smvSelectMinus(*[c + "_pctrk" for c in all_vars])\
            .drop('raw_scr')

        # With this filter, 287 KoLs total
        return res.where(res.full_scr > 0.95)
