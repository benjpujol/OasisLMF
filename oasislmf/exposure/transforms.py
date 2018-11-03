__all__ = [
    'transform'
]

import os

from ..cmd.cleaners import as_path
from ..utils.exceptions import OasisException

from .csv_trans import Translator

def transform(
    self,
    input_fp,
    trans_fp,
    output_fp,
    val_fp=None
)
    """
    Performs any of the following exposure/accounts file transformations:

        source loc./exp. -> canonical loc./exp.
        source acc. -> canonical acc.
        canonical loc./exp. -> model loc./exp.

    :param input_fp: The source file path
    :type input_fp: str

    :param trans_fp: The XSLT transformation file path
    :type trans_fp: str

    :param output_fp: The output file path
    :type output_fp: str

    :param val_fp: The optional validation file path
    :type val_fp: str

    :return: The path to the output canonical file
    """
    try:
        _input_fp = as_path(input_fp, 'input file path')
        _trans_fp = as_path(trans_fp, 'transformation file path')
        _output_fp = as_path(output_fp, 'output file path', preexists=False)
        _val_fp = as_path(val_fp, 'validation file path')
    except OasisException as e:
        raise

    Translator(_input_fp, _output_fp, _trans_fp, xsd_path=_val_fp, append_row_nums=True)()

    return _output_fp

