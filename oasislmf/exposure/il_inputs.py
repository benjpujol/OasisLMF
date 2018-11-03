"""
Insured loss (IL) inputs
"""

__all__ = [
    'generate_il_items',
    'load_il_items',
    'write_il_policytc_file',
    'write_il_profile_file',
    'write_il_programme_file',
    'write_ilsummaryxref_file',
    'write_il_xref_file',
    'write_il_files'
]


import copy
import io
import itertools
import multiprocessing
import sys

import pandas as pd
import six

from ..cmd.cleaners import as_path
from ..utils.concurrency import (
    multiprocess,
    multithread,
    Task,
)
from ..utils.exceptions import OasisException

from ..utils.il import (
    get_level_il_terms
    get_policytc_ids,
)
from ..utils.profiles import combined_grouped_canonical_profile

from .gul_inputs import load_gul_items


def generate_il_items(
    self,
    canexp_profile,
    canexp_df,
    gul_items_df,
    canacc_profile,
    canacc_df,
    il_agg_profile
):
    """
    Generates IL/FM items.

    :param canexp_profile: Canonical exposure profile
    :type canexp_profile: dict

    :param canexp_df: Canonical exposure dataframe
    :type canexp_df: pandas.DataFrame

    :param gul_items_df: GUL items
    :type gul_items_df: pandas.DataFrame

    :param canacc_profile: Canonical accounts profile
    :type canacc_profile: dict

    :param canacc_df: Canonical accounts dataframe
    :param canacc_df: pandas.DataFrame

    :param il_agg_profile: IL/IL/FM aggregation profile
    :param il_agg_profile: dict
    """
    cep = canexp_profile
    cap = canacc_profile
    
    for df in [canexp_df, gul_items_df, canacc_df]:
        if not df.columns.contains('index'):
            df['index'] = pd.Series(data=range(len(df)))

    oed_acc_col_repl = [{'accnumber': 'accntnum'}, {'polnumber': 'policynum'}]
    for repl in oed_acc_col_repl:
            canacc_df.rename(columns=repl, inplace=True)

    cangul_df = pd.merge(canexp_df, gul_items_df, left_on='index', right_on='canexp_id')
    cangul_df['index'] = pd.Series(data=cangul_df.index)

    keys = (
        'item_id', 'gul_item_id', 'peril_id', 'coverage_type_id', 'coverage_id',
        'canexp_id', 'canacc_id', 'policy_num', 'level_id', 'layer_id',
        'agg_id', 'policytc_id', 'deductible', 'deductible_min',
        'deductible_max', 'attachment', 'limit', 'share', 'calcrule_id', 'tiv_elm',
        'tiv', 'tiv_tgid', 'ded_elm', 'ded_min_elm', 'ded_max_elm',
        'lim_elm', 'shr_elm',
    )

    try:
        cgcp = combined_grouped_canonical_profile(profiles=(cep, cap,))

        if not cgcp:
            raise OasisException(
                'Canonical exp. and/or acc. profiles are possibly missing IL/IL/FM term information: '
                'definitions for TIV, limit, blanket deductible, blanket min. deductible, blanket max. deductibe, and/or share are required'
            )

        ilap = il_agg_profile

        if not ilap:
            raise OasisException(
                'IL/IL/FM aggregation profile is empty - this is required to perform aggregation'
            )

        il_levels = tuple(cgcp.keys())

        cov_level_id = il_levels[0]

        coverage_level_preset_data = [t for t in zip(
            tuple(cangul_df.item_id.values),          # 1 - IL/FM item ID
            tuple(cangul_df.item_id.values),          # 2 - GUL item ID
            tuple(cangul_df.peril_id.values),         # 3 - peril ID
            tuple(cangul_df.coverage_type_id.values), # 4 - coverage type ID
            tuple(cangul_df.coverage_id.values),      # 5 - coverage ID
            tuple(cangul_df.canexp_id.values),        # 6 - can. exp. DF index
            (-1,)*len(cangul_df),                     # 7 - can. acc. DF index
            (-1,)*len(cangul_df),                     # 8 - can. acc. policy num.
            (cov_level_id,)*len(cangul_df),           # 9 - coverage level ID
            (1,)*len(cangul_df),                      # 10 - layer ID
            (-1,)*len(cangul_df),                     # 11 - agg. ID
            tuple(cangul_df.tiv_elm.values),          # 12 - TIV element
            tuple(cangul_df.tiv.values),              # 13 -TIV value
            tuple(cangul_df.tiv_tgid.values),         # 14 -TIV element profile term group ID
            tuple(cangul_df.ded_elm.values),          # 15 -deductible element
            tuple(cangul_df.ded_min_elm.values),      # 16 -deductible min. element
            tuple(cangul_df.ded_max_elm.values),      # 17 -deductible max. element
            tuple(cangul_df.lim_elm.values),          # 18 -limit element
            tuple(cangul_df.shr_elm.values)           # 19 -share element
        )]

        def get_canacc_item(i): return canacc_df[(canacc_df['accntnum'] == cangul_df[cangul_df['canexp_id']==coverage_level_preset_data[i][5]].iloc[0]['accntnum'])].iloc[0]

        def get_canacc_id(i): return int(get_canacc_item(i)['index'])

        coverage_level_preset_items = {
            i: {
                k:v for k, v in zip(
                    keys,
                    [i + 1, gul_item_id, peril_id, coverage_type_id, coverage_id, canexp_id, get_canacc_id(i), policy_num, level_id, layer_id, agg_id, -1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 12, tiv_elm, tiv, tiv_tgid, ded_elm, ded_min_elm, ded_max_elm, lim_elm, shr_elm]
                )
            } for i, (item_id, gul_item_id, peril_id, coverage_type_id, coverage_id, canexp_id, _, policy_num, level_id, layer_id, agg_id, tiv_elm, tiv, tiv_tgid, ded_elm, ded_min_elm, ded_max_elm, lim_elm, shr_elm) in enumerate(coverage_level_preset_data)
        }

        num_cov_items = len(coverage_level_preset_items)

        preset_items = {
            level_id: (coverage_level_preset_items if level_id == cov_level_id else copy.deepcopy(coverage_level_preset_items)) for level_id in il_levels
        }

        for i, (level_id, item_id, it) in enumerate(itertools.chain((level_id, k, v) for level_id in il_levels[1:] for k, v in preset_items[level_id].items())):
            it['level_id'] = level_id
            it['item_id'] = num_cov_items + i + 1
            it['ded_elm'] = it['ded_min_elm'] = it['ded_max_elm'] = it['lim_elm'] = it['shr_elm'] = None

        num_sub_layer_level_items = sum(len(preset_items[level_id]) for level_id in il_levels[:-1])
        layer_level = max(il_levels)
        layer_level_items = copy.deepcopy(preset_items[layer_level])
        layer_level_min_idx = min(layer_level_items)

        def layer_id(i): return list(
            canacc_df[canacc_df['accntnum'] == canacc_df.iloc[i]['accntnum']]['policynum'].values
        ).index(canacc_df.iloc[i]['policynum']) + 1

        for i, (canexp_id, canacc_id) in enumerate(
            itertools.chain((canexp_id, canacc_id) for canexp_id in layer_level_items for canexp_id, canacc_id in itertools.product(
                [canexp_id],
                canacc_df[canacc_df['accntnum'] == canacc_df.iloc[layer_level_items[canexp_id]['canacc_id']]['accntnum']]['index'].values)
            )
        ):
            it = copy.deepcopy(layer_level_items[canexp_id])
            it['item_id'] = num_sub_layer_level_items + i + 1
            it['layer_id'] = layer_id(canacc_id)
            it['canacc_id'] = canacc_id
            preset_items[layer_level][layer_level_min_idx + i] = it

        for it in (it for c in itertools.chain(six.itervalues(preset_items[k]) for k in preset_items) for it in c):
            it['policy_num'] = canacc_df.iloc[it['canacc_id']]['policynum']
            lilaggkey = ilap[it['level_id']].get('FMAggKey') or ilap[it['level_id']].get('AggKey')
            for v in six.itervalues(lilaggkey):
                src = v['src'].lower()
                if src in ['canexp', 'canacc']:
                    f = v['field'].lower()
                    it[f] = canexp_df.iloc[it['canexp_id']][f] if src == 'canexp' else canacc_df.iloc[it['canacc_id']][f]

        concurrent_tasks = (
            Task(get_level_il_terms, args=(cgcp[level_id], ilap[level_id], preset_items[level_id], canexp_df.copy(deep=True), canacc_df.copy(deep=True),), key=level_id)
            for level_id in il_levels
        )
        num_ps = min(len(il_levels), multiprocessing.cpu_count())
        for it in multiprocess(concurrent_tasks, pool_size=num_ps):
            yield it
    except (AttributeError, KeyError, IndexError, TypeError, ValueError) as e:
        raise OasisException(e)


def load_il_items(
    self,
    canexp_profile,
    canexp_df,
    gul_items_df,
    canacc_profile,
    canacc_fp,
    il_agg_profile,
    reduced=True
):
    """
    Loads IL items generated by ``generate_il_items`` into a Pandas frame,
    with some post-processing, including reducing the table by removing
    items with empty IL terms and setting the policy TC IDs.

    :param canexp_profile: Canonical exposure profile
    :type canexp_profile: dict

    :param canexp_df: Canonical exposure dataframe
    :type canexp_df: pandas.DataFrame

    :param gul_items_df: GUL items
    :type gul_items_df: pandas.DataFrame

    :param canacc_profile: Canonical accounts profile
    :type canacc_profile: dict

    :param canacc_fp: Canonical accounts file path
    :param canacc_fp: str

    :param il_agg_profile: IL/IL/FM aggregation profile
    :param il_agg_profile: dict

    :param reduced: Whether to drop IL items with zero values for limits,
                   deductibles and shares. By default ``True``
    :param reduced: bool
    """
    cep = canexp_profile
    cap = canacc_profile

    ilap = il_agg_profile

    try:
        with io.open(canacc_fp, 'r', encoding='utf-8') as f:
            canacc_df = pd.read_csv(f, float_precision='high')

        if len(canacc_df) == 0:
            raise OasisException('No canonical accounts items')
        
        canacc_df = canacc_df.where(canacc_df.notnull(), None)
        canacc_df.columns = canacc_df.columns.str.lower()
        canacc_df['index'] = pd.Series(data=range(len(canacc_df)))

        il_items = [it for it in generate_il_items(canexp_df, gul_items_df, cep, cap, canacc_df, ilap)]
        il_items.sort(key=lambda it: it['item_id'])

        il_items_df = pd.DataFrame(data=il_items, dtype=object)
        il_items_df['index'] = pd.Series(data=range(len(il_items_df)))

        bookend_levels = (il_items_df['level_id'].min(), il_items_df['level_id'].max(),)

        if reduced:
            il_items_df = il_items_df[(il_items_df['level_id'].isin(bookend_levels)) | (il_items_df['limit'] != 0) | (il_items_df['deductible'] != 0) | (il_items_df['deductible_min'] != 0) | (il_items_df['deductible_max'] != 0) | (il_items_df['share'] != 0)]

            il_items_df['index'] = range(len(il_items_df))

            il_items_df['item_id'] = range(1, len(il_items_df) + 1)

            level_ids = [l for l in set(il_items_df['level_id'])]

            level_id = lambda i: level_ids.index(il_items_df.iloc[i]['level_id']) + 1

            il_items_df['level_id'] = il_items_df['index'].apply(level_id)

        layer_level_id = il_items_df['level_id'].max()

        policytc_ids = get_policytc_ids(il_items_df)
        def get_policytc_id(i):
            return [
                k for k in six.iterkeys(policytc_ids) if policytc_ids[k] == {k:il_items_df.iloc[i][k] for k in ('limit', 'deductible', 'attachment', 'deductible_min', 'deductible_max', 'share', 'calcrule_id',)}
            ][0]
        il_items_df['policytc_id'] = il_items_df['index'].apply(lambda i: get_policytc_id(i))

        for col in il_items_df.columns:
            if col.endswith('id'):
                il_items_df[col] = il_items_df[col].astype(int)
            elif col in ('tiv', 'limit', 'deductible', 'deductible_min', 'deductible_max', 'share',):
                il_items_df[col] = il_items_df[col].astype(float)
    except (IOError, MemoryError, OasisException, OSError, TypeError, ValueError) as e:
        raise OasisException(e)

    return il_items_df, canacc_df


def write_policytc_file(self, il_items_df, policytc_fp):
    """
    Writes an IL/FM policy T & C file.
    """
    try:
        policytc_df = pd.DataFrame(
            columns=['layer_id', 'level_id', 'agg_id', 'policytc_id'],
            data=[key[:4] for key, _ in il_items_df.groupby(['layer_id', 'level_id', 'agg_id', 'policytc_id', 'limit', 'deductible', 'deductible_min', 'deductible_max', 'share'])],
            dtype=object
        )
        policytc_df.to_csv(
            path_or_buf=policytc_fp,
            encoding='utf-8',
            chunksize=1000,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException(e)

    return policytc_fp


def write_profile_file(self, il_items_df, profile_fp):
    """
    Writes an IL/FM profile file.
    """
    try:
        cols = ['policytc_id', 'calcrule_id', 'limit', 'deductible', 'deductible_min', 'deductible_max', 'attachment', 'share']

        profile_df = il_items_df[cols]

        profile_df = pd.DataFrame(
            columns=cols,
            data=[key for key, _ in profile_df.groupby(cols)]
        )

        col_repl = [
            {'deductible': 'deductible1'},
            {'deductible_min': 'deductible2'},
            {'deductible_max': 'deductible3'},
            {'attachment': 'attachment1'},
            {'limit': 'limit1'},
            {'share': 'share1'}
        ]
        for repl in col_repl:
            profile_df.rename(columns=repl, inplace=True)

        n = len(profile_df)

        profile_df['index'] = range(n)

        profile_df['share2'] = profile_df['share3'] = [0]*n

        profile_df.to_csv(
            columns=['policytc_id','calcrule_id','deductible1', 'deductible2', 'deductible3', 'attachment1', 'limit1', 'share1', 'share2', 'share3'],
            path_or_buf=profile_fp,
            encoding='utf-8',
            chunksize=1000,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException(e)

    return profile_fp


def write_programme_file(self, il_items_df, programme_fp):
    """
    Writes a IL/FM programme file.
    """
    try:
        il_aggtree = {
            key:set(group['agg_id']) for key, group in il_items_df[['level_id', 'agg_id']].groupby(['level_id'])
        }
        levels = sorted(il_aggtree.keys())
        il_aggtree[0] = il_aggtree[levels[0]]
        levels = sorted(il_aggtree.keys())

        data = [
            (a, second, b) for first, second in zip(levels, levels[1:]) for a, b in (
                zip(il_aggtree[first], il_aggtree[second]) if (len(il_aggtree[first]) == len(il_aggtree[second]) and len(il_aggtree[first]) > 1) else itertools.product(il_aggtree[first], [list(il_aggtree[second])[0]])
            )
        ]

        programme_df = pd.DataFrame(columns=['from_agg_id', 'level_id', 'to_agg_id'], data=data, dtype=int)

        programme_df.to_csv(
            path_or_buf=programme_fp,
            encoding='utf-8',
            chunksize=1000,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException(e)

    return programme_fp


def write_xref_file(self, il_items_df, xref_fp):
    """
    Writes a IL/FM xref file.
    """
    try:
        data = [
            (i + 1, agg_id, layer_id) for i, (agg_id, layer_id) in enumerate(itertools.product(set(il_items_df['agg_id']), set(il_items_df['layer_id'])))
        ]

        xref_df = pd.DataFrame(columns=['output', 'agg_id', 'layer_id'], data=data, dtype=int)

        xref_df.to_csv(
            path_or_buf=xref_fp,
            encoding='utf-8',
            chunksize=1000,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException(e)

    return xref_fp


def write_summaryxref_file(self, il_items_df, summaryxref_fp):
    """
    Writes an IL/FM summaryxref file.
    """
    try:
        data = [
            (i + 1, 1, 1) for i, _ in enumerate(itertools.product(set(il_items_df['agg_id']), set(il_items_df['layer_id'])))
        ]

        summaryxref_df = pd.DataFrame(columns=['output', 'summary_id', 'summaryset_id'], data=data, dtype=int)

        summaryxref_df.to_csv(
            path_or_buf=summaryxref_fp,
            encoding='utf-8',
            chunksize=1000,
            index=False
        )
    except (IOError, OSError) as e:
        raise OasisException(e)

    return summaryxref_fp


def write_il_files(
    self,
    canexp_profile,
    canexp_df,
    gul_items_df,
    canacc_profile,
    canacc_fp,
    il_agg_profile,
    policytc_fp,
    profile_fp,
    programme_fp,
    xref_fp,
    summaryxref_fp
):
    """
    Writes the standard Oasis IL/FM files, namely::

        fm_policytc.csv
        fm_profile.csv
        fm_programm.ecsv
        fm_xref.csv
        fm_summaryxref.csv
    """
    if not (policytc_fp and profile_fp and programme_fp and xref_fp and summaryxref_fp):
        raise OasisException('At least one or more of the IL file paths is missing - policy TC, profile, programme, xref and summaryxref file paths are all required')

    il_items_df, canacc_df = load_il_items(canexp_profile, canexp_df, gul_items_df, canacc_profile, canacc_fp, il_agg_profile)

    il_files = {
        'policytc': policytc_fp,
        'profile': profile_fp,
        'programme': programme_fp,
        'xref': xref_fp,
        'summaryxref': summaryxref_fp
    }

    concurrent_tasks = (
        Task(getattr(self, 'write_{}_file'.format(f)), args=(fm_items_df.copy(deep=True), il_files[f],), key=f)
        for f in il_files
    )
    num_ps = min(len(il_files), multiprocessing.cpu_count())
    n = len(il_files)
    for _, _ in multithread(concurrent_tasks, pool_size=num_ps):
        pass

    return il_files
