import json
from collections import Counter
from tempfile import NamedTemporaryFile
from unittest import TestCase

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
from filecmp import dircmp

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
        