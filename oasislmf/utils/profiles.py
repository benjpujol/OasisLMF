__all__ = [
    'get_combined_grouped_canonical_profile',
    'get_il_aggregation_profile'
]

import io
import json

import six

from ..utils.exceptions import OasisException


def get_il_aggregation_profile(self, profile_json=None, profile_path=None):
    """
    Gets the IL aggregation profile from a file path or JSON. This is used to
    aggregate/group IL items during the process of generating IL items.
    """
    if not (profile_json or profile_path):
        raise OasisException('An insured loss (IL) aggregation profile JSON or path must be provided')

    if profile_json:
        return {int(k):v for k, v in six.iteritems(json.loads(profile_json))}

    with io.open(profile_path, 'r', encoding='utf-8') as f:
        return {int(k):v for k, v in six.iteritems(json.load(f))}


def get_combined_grouped_canonical_profile(profiles=[], profile_paths=[]):
    """
    Gets the combined and grouped canonical profile from a set of canonical
    loc. and/or acc. profiles - the combination merges the two profiles (if
    two profiles are provided) into one and the grouping is first by FM level
    and then by FM term group.
    """

    if not (profiles or profile_paths):
        raise OasisException('A list of canonical profiles (loc. or acc.), or a list or tuple of canonical profiles paths must be provided')

    if not profiles:
        for pp in profile_paths:
            with io.open(pp, 'r', encoding='utf-8') as f:
                profiles.append(json.load(f))

    comb_prof = {k:v for p in profiles for k, v in ((k, v) for k, v in six.iteritems(p) if 'FMLevel' in v)}

    return {
        k:{
            _k:{v['FMTermType'].lower():v for v in g} for _k, g in itertools.groupby(sorted(six.itervalues(comb_prof[k]), key=lambda v: v['FMTermGroupID']), key=lambda v: v['FMTermGroupID'])
        } for k in comb_prof
    }