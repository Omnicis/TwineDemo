from smv import SmvGroupedData
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, first

#class TestH(SmvCsvStringData):
#    def schemaStr(self):
#        return "zip:String;Territory:String;Division:String;Division_name:String"
#
#    def dataStr(self):
#        return """100,001,01,D01;
#            102,001,01,D01;
#            103,001,01,D01;
#            201,002,01,D01;
#            301,003,02,D02;
#            401,004,02,D02;
#            405,004,02,D02"""
#
class HierarchyUtils(object):
    """Helper class for apply a hierarchy map to a DF and do aggregations

        Args:
            prefix (str): the type of the hierarchies, which will be prefix of
                the generated hierarchy columns. For example, when prefix="geo",
                columns, "geo_type", "geo_value", "geo_name" will be created when
                apply the hierarchy to a DataFrame.
            hiers (\*(str, list(str))): pairs of hierarchy name and hierarchy list.
                The hierarchy list has the column names from smaller unit to larger unit.
                Please note that the first(smallest) hierarchy level of all the hierarchy
                lists have to be the same.

        Example:
            >>> h = HierarchyUtils("geo",
            ...         ("zip3", ["zip", "zip3", "state", "country"]),
            ...         ("county", ["zip", "county", "state", "country"]))

    """
    def __init__(self, prefix, *hiers):
        self.prefix = prefix
        self.hiers = hiers
        keys = set([h[0] for (n, h) in hiers])
        if (len(keys) > 1):
            raise RuntimeError("The first level of all the hierarchy lists have to be the same")
        else:
            self.key = keys.pop()

    def _apply_to_df(self, df, hier_map):
        return df.smvJoinByKey(hier_map, [self.key], "inner")

    def _tcol_name(self):
        return self.prefix + "_type"

    def _vcol_name(self):
        return self.prefix + "_value"

    def _ncol_name(self):
        return self.prefix + "_name"

    def _ncol_name_in_map(self, type_name):
        return type_name + "_name"

    def _hier_list(self, hier_name):
        return [r for (n, r) in self.hiers if n == hier_name][0]

    def _parent_hier(self, hier_name, l):
        hlist = self._hier_list(hier_name)
        return dict(zip(hlist, (hlist[1:] + [None]))).get(l)

    def _parent_prefix(self):
        return "parent_"

    def hier_agg(self, smvgd, hier_map, *levels):
        """Aggregate the SmvGroupedData with the additional hierarchy level.
            Return a DF with `prefix_type`, `prefix_value`, `prefix_name`.

            Args:
                smvgd (DataFrame or SmvGroupedData): Data to be aggregated with hierarchy
                hier_map (DataFrame): The hierarchy map table, which has all hierarchy
                    levels as columns, and optionally has `level_name` for all the levels
                    as the level names.
                levels (\*str): hierarchy levels to be aggregated on

            Example:
                >>> tx_county_sum = my_geo_hier.hier_agg(tx, zipMap, "county", "state")(
                ...     sum("amt").alias("total_amt"),
                ...     sum("qty").alias("total_qty"))

            Returns:
                (function): a function takes Spark aggregations as parameters, and returns
                    a DF with columns of
                    * keys from the SmvGroupedData
                    * prefix_type, prefix_value, prefix_name
                    * aggregated columns

        """
        if (isinstance(smvgd, DataFrame)):
            df = smvgd
            origkeys = []
        elif (isinstance(smvgd, SmvGroupedData)):
            df = smvgd.df
            origkeys = smvgd.keys
        else:
            raise RuntimeError("agg method takes DataFrame or SmvGroupedData as its first parameter")

        applied = self._apply_to_df(df, hier_map)

        def _df_reorder_cols(df, keys):
            hier_col_names = [self._tcol_name(), self._vcol_name(), self._ncol_name()]
            other_col_names = [i for i in df.columns if i not in (hier_col_names + keys)]
            return df.select(*(keys + hier_col_names + other_col_names))

        def _agg_inner(*aggcols):
            # TODO: The following can be optimized: higher lever hierarchy aggregation can based on
            # lower level aggregation results, if the agg functions are addable.
            def do_agg(level):
                ncol_in_map = self._ncol_name_in_map(level)
                if (ncol_in_map in hier_map.columns):
                    aggcols_with_name = [first(ncol_in_map).alias(self._ncol_name())] + list(aggcols)
                else:
                    aggcols_with_name = [lit(None).cast("string").alias(self._ncol_name())] + list(aggcols)
                keys = origkeys + [level]
                aggres = applied.groupBy(*keys).agg(*aggcols_with_name)
                res = aggres\
                    .withColumn(self._tcol_name(), lit(level))\
                    .withColumnRenamed(level, self._vcol_name())

                return _df_reorder_cols(res, origkeys)
            return reduce(lambda l, r: l.unionAll(r), [do_agg(l) for l in levels])

        return _agg_inner

    def add_parent_keys(self, df, hier_map, hier_name):
        """Add parent hierarchy's type, value, name to an output df from hier_agg method.

            For each hierarchy level, we define its parent hierarchy level along a given `hier_name`
            as the next hierarchy level in the hierarchy list with the given `hier_name`.
            For example, for the following HierarchyUtils:

                >>> h = HierarchyUtils("geo",
                ...         ("zip3", ["zip", "zip3", "state", "country"]),
                ...         ("county", ["zip", "county", "state", "country"]))

            Along the hier_name "zip3", the parent hierarchy level of "zip" is "zip3", the parent
            hierarchy level of "zip3" is "state", etc.

            Args:
                df (DataFrame): DataFrame as an output of `hier_agg` method
                hier_map (DataFrame): The hierarchy map to be applied
                hier_name (str): The name of the hierarchy list, alone which parent hierarchies are
                    defined

            Returns:
                (DataFrame): DataFrame with the following column appended
                    * parent_$\{prefix\}_type
                    * parent_$\{prefix\}_value
                    * parent_$\{prefix\}_name
        """

        def select_cols_with_parent(h):
            p = self._parent_hier(hier_name, h)
            p_prefix = self._parent_prefix()

            def ncol(level):
                ncol_in_map = self._ncol_name_in_map(level)
                if (ncol_in_map in hier_map.columns):
                    ncol = col(ncol_in_map)
                else:
                    ncol = lit(None).cast("string")
                return ncol

            return hier_map.select(
                lit(h).alias(self._tcol_name()),
                col(h).alias(self._vcol_name()),
                lit(p).cast("string").alias(p_prefix + self._tcol_name()),
                (col(p) if (p is not None) else lit(None).cast("string")).alias(p_prefix + self._vcol_name()),
                (ncol(p) if (p is not None) else lit(None).cast("string")).alias(p_prefix + self._ncol_name())
            ).smvDedupByKey(self._tcol_name(), self._vcol_name())

        map_with_parent = reduce(
            lambda l, r: l.smvUnion(r),
            [select_cols_with_parent(h) for h in self._hier_list(hier_name)]
        )

        return df.smvJoinByKey(map_with_parent, [self._tcol_name(), self._vcol_name()], "leftouter")

    def add_parent_vals(self, smvgd, hier_map, hier_name):
        """Add parent hierarchy values to a result DF of hier_agg method.

        Example:
            >>> tx_county_sum = my_geo_hier.hier_agg(tx, zipMap, "county", "state")(
            ...     sum("amt").alias("total_amt"),
            ...     sum("qty").alias("total_qty"))
            >>> tx_county_sum_with_p = my_geo_hier.add_parent_vals(tx_county_sum, zipMap, "county")

        The output DF will have `parent_total_amt` and `parent_total_qty` appended as long as the
        parent keys (`parent_prefix_type`, `parent_prefix_value`, `parent_prefix_name`)

        Args:
            df (DataFrame): a result DF of hier_agg method with all the needed hierarchy levels for
                the specified hier_name
            hier_map (DataFrame): the hierarchy map table to be applied (if `df` does not have parent
                keys appended yet)
            hier_name (str): the hierarchy list the parent defined on

        Returns:
            (DataFrame): DF with parent hierarchy level values appended
        """

        if (isinstance(smvgd, DataFrame)):
            df = smvgd
            origkeys = []
        elif (isinstance(smvgd, SmvGroupedData)):
            df = smvgd.df
            origkeys = smvgd.keys
        else:
            raise RuntimeError("agg method takes DataFrame or SmvGroupedData as its first parameter")

        if ((self._parent_prefix() + self._tcol_name()) in df.columns):
            df_w_parent_keys = df
        else:
            df_w_parent_keys = self.add_parent_keys(df, hier_map, hier_name)

        # Group column names
        keys = [self._tcol_name(), self._vcol_name(), self._ncol_name()]
        p_keys = [self._parent_prefix() + k for k in keys]
        vals = [i for i in df_w_parent_keys.columns if i not in (p_keys + keys + origkeys)]

        phlist = (self._hier_list(hier_name))[1:]

        right_df = df_w_parent_keys.where(col(self._tcol_name()).isin(phlist))\
            .smvDedupByKey(*(origkeys + keys))
        right_df_renamed = right_df.select(*(
            origkeys +
            [col(k).alias(self._parent_prefix() + k) for k in keys] +
            [col(v).alias(self._parent_prefix() + v) for v in vals]
        ))

        return df_w_parent_keys.smvJoinByKey(
            right_df_renamed,
            origkeys + p_keys,
            'leftouter'
        )
