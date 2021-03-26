import numpy as np
from pyspark.sql import DataFrame

def get_reduced_columns(df: DataFrame, threshold=0.5, remove_negative=False):
    pandas_df = df.toPandas()
    print("All Features: %s" % len(pandas_df.columns))
    reduced_feature_set = _find_correlation(pandas_df, threshold, remove_negative)
    print("Reduced Features: %s" % len(reduced_feature_set))
    return list(reduced_feature_set)


def _find_correlation(data, threshold: float, remove_negative: bool):
    """
    Given a numeric pd.DataFrame, this will find highly correlated features,
    and return a list of features to remove.
    Parameters
    -----------
    data : pandas DataFrame
        DataFrame
    threshold : float
        correlation threshold, will remove one of pairs of features with a
        correlation greater than this value.
    remove_negative: Boolean
        If true then features which are highly negatively correlated will
        also be returned for removal.
    Returns
    --------
    select_flat : list
        listof column names to be removed
    """
    corr_mat = data.corr()
    if remove_negative:
        corr_mat = np.abs(corr_mat)
    corr_mat.loc[:, :] = np.tril(corr_mat, k=-1)
    already_in = set()
    result = []
    for col in corr_mat:
        perfect_corr = corr_mat[col][corr_mat[col] > threshold].index.tolist()
        if perfect_corr and col not in already_in:
            already_in.update(set(perfect_corr))
            perfect_corr.append(col)
            result.append(perfect_corr)
    select_nested = [f[1:] for f in result]
    select_flat = [i for j in select_nested for i in j]
    return set(select_flat)
