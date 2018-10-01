# -*- coding: utf-8 -*-
import importlib
import io
import json
import logging
import os
import subprocess
import sys

from argparse import RawDescriptionHelpFormatter

from pathlib2 import Path

from ..exposures.csv_trans import Translator
from ..exposures.manager import OasisExposuresManager

from ..model_execution.bash import genbash
from ..model_execution import runner
from ..model_execution.bin import create_binary_files, prepare_model_run_directory, prepare_model_run_inputs

from ..utils.exceptions import OasisException
from ..utils.peril import PerilAreasIndex
from ..utils.values import get_utctimestamp

from ..keys.lookup import OasisLookupFactory

def generate_oasis_files(
    default_oasis_files_path,
    oasis_files_path,
    lookup_config_fp,
    keys_data_path,
    model_version_file_path,
    lookup_package_path,
    canonical_exposures_profile_json_path,
    source_exposures_file_path,
    source_exposures_validation_file_path,
    source_to_canonical_exposures_transformation_file_path,
    canonical_exposures_validation_file_path,
    canonical_to_model_exposures_transformation_file_path,    
    no_timestamp = False,
    logger=logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)):

    if not (lookup_config_fp or (keys_data_path and model_version_file_path and lookup_package_path)):
        raise OasisException('Either the lookup config JSON file path or the keys data path + model version file path + lookup package path must be provided')

    logger.info('\nGetting model info and lookup')
    model_info, lookup = OasisLookupFactory.create(
            lookup_config_fp=lookup_config_fp,
            model_keys_data_path=keys_data_path,
            model_version_file_path=model_version_file_path,
            lookup_package_path=lookup_package_path
    )
    logger.info('\t{}, {}'.format(model_info, lookup))

    logger.info('\nCreating Oasis model object')
    oasis_exposure_manager = OasisExposuresManager(do_timestamp = not no_timestamp)
    model = oasis_exposure_manager.create(
        model_supplier_id=model_info['supplier_id'],
        model_id=model_info['model_id'],
        model_version=model_info['model_version'],
        resources={
            'lookup': lookup,
            'lookup_config_fp': lookup_config_fp or None,
            'oasis_files_path': oasis_files_path,
            'source_exposures_file_path': source_exposures_file_path,
            'source_exposures_validation_file_path': source_exposures_validation_file_path,
            'source_to_canonical_exposures_transformation_file_path': source_to_canonical_exposures_transformation_file_path,
            'canonical_exposures_profile_json_path': canonical_exposures_profile_json_path,
            'canonical_exposures_validation_file_path': canonical_exposures_validation_file_path,
            'canonical_to_model_exposures_transformation_file_path': canonical_to_model_exposures_transformation_file_path
        }
    )
    logger.info('\t{}'.format(model))

    logger.info('\nSetting up Oasis files directory for model {}'.format(model.key))
    Path(oasis_files_path).mkdir(parents=True, exist_ok=True)

    logger.info('\nGenerating Oasis files for model')
    oasis_files = oasis_exposure_manager.start_files_pipeline(
        oasis_model=model,
        logger=logger,
    )

    logger.info('\nGenerated Oasis files for model: {}'.format(oasis_files))


def generate_losses(
    oasis_files_path,
    model_run_dir_path,
    analysis_settings_json_file_path,
    model_data_path,
    model_package_path,
    ktools_script_name,
    ktools_num_processes,
    no_execute = False,
    logger=logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)):

    if not model_run_dir_path:
        utcnow = get_utctimestamp(fmt='%Y%m%d%H%M%S')
        model_run_dir_path = os.path.join(os.getcwd(), 'runs', 'ProgOasis-{}'.format(utcnow))
        logger.info('\nNo model run dir. provided - creating a timestamped run dir. in working directory as {}'.format(model_run_dir_path))
        Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)
    else:
        if not os.path.exists(model_run_dir_path):
            Path(model_run_dir_path).mkdir(parents=True, exist_ok=True)

    logger.info(
        '\nPreparing model run directory {} - copying Oasis files, analysis settings JSON file and linking model data'.format(model_run_dir_path)
    )
    prepare_model_run_directory(
        model_run_dir_path,
        oasis_files_path,
        analysis_settings_json_file_path,
        model_data_path
    )

    logger.info('\nConverting Oasis files to ktools binary files')
    oasis_files_path = os.path.join(model_run_dir_path, 'input', 'csv')
    binary_files_path = os.path.join(model_run_dir_path, 'input')
    create_binary_files(oasis_files_path, binary_files_path)

    analysis_settings_json_file_path = os.path.join(model_run_dir_path, 'analysis_settings.json')
    try:
        logger.info('\nReading analysis settings JSON file')
        with io.open(analysis_settings_json_file_path, 'r', encoding='utf-8') as f:
            analysis_settings = json.load(f)
            if 'analysis_settings' in analysis_settings:
                analysis_settings = analysis_settings['analysis_settings']
    except (IOError, TypeError, ValueError):
        raise OasisException('Invalid analysis settings JSON file or file path: {}.'.format(analysis_settings_json_file_path))

    logger.info('\nLoaded analysis settings JSON: {}'.format(analysis_settings))

    logger.info('\nPreparing model run inputs')
    prepare_model_run_inputs(analysis_settings, model_run_dir_path)

    script_path = os.path.join(model_run_dir_path, '{}.sh'.format(ktools_script_name))
    if no_execute:
        logger.info('\nGenerating ktools losses script')
        genbash(
            ktools_num_processes,
            analysis_settings,
            filename=script_path,
        )
        logger.info('\nMaking ktools losses script executable')
        subprocess.check_call("chmod +x {}".format(script_path), stderr=subprocess.STDOUT, shell=True)
    else:
        os.chdir(model_run_dir_path)

        if model_package_path and os.path.exists(os.path.join(model_package_path, 'supplier_model_runner.py')):
            path, package_name = model_package_path.rsplit('/')
            sys.path.append(path)
            model_runner_module = importlib.import_module('{}.supplier_model_runner'.format(package_name))
        else:
            model_runner_module = runner

        model_runner_module.run(analysis_settings, ktools_num_processes, filename=script_path)

    logger.info('\nLoss outputs generated in {}'.format(os.path.join(model_run_dir_path, 'output')))


def transform_source_to_canonical(
    source_file_path, output_file_path, xslt_transformation_file_path, xsd_validation_file_path,
    logger=logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)):

    logger.info('\nGenerating a canonical file {} from source file {}'.format(output_file_path, source_file_path))
    translator = Translator(source_file_path, output_file_path, xslt_transformation_file_path, xsd_validation_file_path, append_row_nums=True)
    translator()
    logger.info('\nOutput file {} successfully generated'.format(output_file_path))

def transform_canonical_to_model(
    canonical_exposures_file_path, output_file_path, xslt_transformation_file_path, xsd_validation_file_path,
    logger=logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)):

    logger.info('\nGenerating a model exposures file {} from canonical exposures file {}'.format(output_file_path, canonical_exposures_file_path))
    translator = Translator(canonical_exposures_file_path, output_file_path, xslt_transformation_file_path, xsd_validation_file_path ,append_row_nums=True)
    translator()
    logger.info('\nOutput file {} successfully generated'.format(output_file_path))
