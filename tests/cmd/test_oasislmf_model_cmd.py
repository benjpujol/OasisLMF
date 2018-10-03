import json
from collections import Counter
from tempfile import NamedTemporaryFile
from unittest import TestCase

import io
import os

import shutil
import six
from hypothesis import (
    given,
    HealthCheck,
    settings,
)
from hypothesis.strategies import sampled_from, text, dictionaries, booleans, integers
from mock import Mock, ANY, patch
from backports.tempfile import mkdtemp, TemporaryDirectory
from pathlib2 import Path

from oasislmf.api_client.client import OasisAPIClient
from oasislmf.cmd import RootCmd
from oasislmf.cmd.test import TestModelApiCmd
from oasislmf.utils.exceptions import OasisException

from argparse import Namespace
import oasislmf.cmd.model
from oasislmf.workflow.model import generate_oasis_files_from_canonical_and_keys

from filecmp import dircmp, cmp

import sys
import logging

TEST_DIRECTORY = os.path.dirname(__file__)

CMD_INPUT_FOLDER = os.path.join(TEST_DIRECTORY, "command_input")
CMD_OUTPUT_FOLDER = os.path.join(TEST_DIRECTORY, "command_output")
CMD_REFERENCE_FOLDER = os.path.join(TEST_DIRECTORY, "command_reference")

class TestOasislmfModelCmd(TestCase):

    def dir_match(self, dir1, dir2):
        compared = dircmp(dir1, dir2)
        if (compared.left_only or compared.right_only or compared.diff_files 
           or compared.funny_files):
           return False
        for subdir in compared.common_dirs:
            if not self.dir_match(os.path.join(dir1, subdir), os.path.join(dir2, subdir)):
                return False
        return True

    def file_match(self, f1, f2):
        return cmp(f1, f2)
    
    @patch("os.getcwd")
    def test_model_run(self, mock_os_getcwd):
        with TemporaryDirectory() as d:
            model_run_dir_name = d
            args = Namespace(
                config=os.path.join(TEST_DIRECTORY, "command_input/oasislmf.json"), 
                ktools_num_processes=1,
                model_run_dir_path=model_run_dir_name
            )
            cmd = oasislmf.cmd.model.RunCmd()
            cmd.action(args)
            output_dir_name = os.path.join(model_run_dir_name, "output")
            reference_dir_name = os.path.join(CMD_REFERENCE_FOLDER, "run")

            self.assertTrue(
                self.dir_match(
                    output_dir_name,
                    reference_dir_name
                )
            )

    @patch("os.getcwd")
    def test_model_generate_oasis_files(self, mock_os_getcwd):
        with TemporaryDirectory() as d:
            oasis_files_path = d
            args = Namespace(
                config=os.path.join(TEST_DIRECTORY, "command_input/oasislmf.json"), 
                ktools_num_processes=1,
                oasis_files_path=oasis_files_path,
                no_timestamp=True
            )
            cmd = oasislmf.cmd.model.GenerateOasisFilesCmd()
            cmd.action(args)
            reference_dir_name = os.path.join(CMD_REFERENCE_FOLDER, "generate_oasis_files")

            self.assertTrue(
                self.dir_match(
                    oasis_files_path,
                    reference_dir_name
                )
            )

    @patch("os.getcwd")
    def test_model_generate_losses(self, mock_os_getcwd):

        with TemporaryDirectory() as d:
            oasis_files_path = os.path.join(CMD_REFERENCE_FOLDER, "generate_oasis_files")
            model_run_dir_name = d
            args = Namespace(
                config=os.path.join(TEST_DIRECTORY, "command_input/oasislmf.json"), 
                ktools_num_processes=1,
                oasis_files_path=oasis_files_path,
                model_run_dir_path=model_run_dir_name
            )
            cmd = oasislmf.cmd.model.GenerateLossesCmd()
            cmd.action(args)
            reference_dir_name = os.path.join(CMD_REFERENCE_FOLDER, "run")

            output_dir_name = os.path.join(model_run_dir_name, "output")
            reference_dir_name = os.path.join(CMD_REFERENCE_FOLDER, "run")

            self.assertTrue(
                self.dir_match(
                    output_dir_name,
                    reference_dir_name
                )
            )
        
    @patch("os.getcwd")
    def test_transform_source_to_canonical(self, mock_os_getcwd):
        with TemporaryDirectory() as d:
            out_file_path = os.path.join(d, "canexp.csv")
            args = Namespace(
                config=os.path.join(TEST_DIRECTORY, "command_input/oasislmf.json"),                 
                source_file_path="data/SourceLocPiWind.csv",
                source_file_type="exposures",
                xsd_validation_file_path="flamingo/PiWind/Files/ValidationFiles/Generic_Windstorm_CanLoc_A.xsd",
                xslt_transformation_file_path="flamingo/PiWind/Files/TransformationFiles/MappingMapToGeneric_Windstorm_CanLoc_A.xslt",
                output_file_path=out_file_path
            )
            cmd = oasislmf.cmd.model.TransformSourceToCanonicalFileCmd()
            cmd.action(args)
            reference_file_path = os.path.join(
                CMD_REFERENCE_FOLDER, "transform_source_to_canonical", "canexp.csv")

            self.assertTrue(
                self.file_match(
                    out_file_path,
                    reference_file_path
                )
            )

    @patch("os.getcwd")
    def test_transform_canonical_to_model(self, mock_os_getcwd):
        with TemporaryDirectory() as d:
            out_file_path = os.path.join(d, "modexp.csv")
            args = Namespace(
                config=os.path.join(TEST_DIRECTORY, "command_input/oasislmf.json"),                 
                canonical_exposures_file_path="canexp.csv",
                xsd_validation_file_path="flamingo/PiWind/Files/ValidationFiles/piwind_modelloc.xsd",
                xslt_transformation_file_path="flamingo/PiWind/Files/TransformationFiles/MappingMapTopiwind_modelloc.xslt",
                output_file_path=out_file_path
            )
            cmd = oasislmf.cmd.model.TransformCanonicalToModelFileCmd()
            cmd.action(args)
            reference_file_path = os.path.join(
                CMD_REFERENCE_FOLDER, "transform_canonical_to_model", "modexp.csv")

            self.assertTrue(
                self.file_match(
                    out_file_path,
                    reference_file_path
                )
            )


    #TODO: This should be in a seperate workflow tests module. 
    @patch("os.getcwd")
    def test_generate_oasis_files_from_canonical_and_keys(self, mock_os_getcwd):
        
        with TemporaryDirectory() as d:
            oasis_files_path = d
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

            canonical_exposures_profile_path = \
                os.path.join(TEST_DIRECTORY, "command_input", "oasislmf-piwind-canonical-loc-profile.json")
            with io.open(canonical_exposures_profile_path, 'r', encoding='utf-8') as f:
                canonical_exposures_profile = json.load(f)
            generate_oasis_files_from_canonical_and_keys(
                oasis_files_path=d,
                keys_file_path=os.path.join(TEST_DIRECTORY, "command_input", "oasiskeys.csv"),
                canonical_exposures_file_path=os.path.join(TEST_DIRECTORY, "command_input", "canexp.csv"),
                canonical_exposures_profile=canonical_exposures_profile,
                logger=logging.getLogger()
            )

            reference_dir_name = os.path.join(CMD_REFERENCE_FOLDER, "generate_oasis_files_from_canonical_and_keys")

            self.assertTrue(
                self.dir_match(
                    oasis_files_path,
                    reference_dir_name
                )
            )        
