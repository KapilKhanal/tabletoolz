#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `tabletoolz` package."""

import pytest

from click.testing import CliRunner

from tabletoolz.pyspark import *
from tabletoolz import cli


@pytest.fixture
def response():
    df = [{"Name":"Kapil","Major":"Data"},{"Name":"Mapil","Major":"Hata"}]
    cols = ["Name"]
    return list(select(df,cols))
    

def test_select(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string
    output = [{'Name': 'Kapil'}, {'Name': 'Mapil'}]
    assert response == output


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'tabletoolz.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output
