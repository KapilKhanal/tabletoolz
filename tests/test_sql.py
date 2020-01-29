"""Tests for `tabletoolz` package."""

import pytest

from click.testing import CliRunner

from tabletoolz import *
from tabletoolz import cli

# import database
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base

engine = create_engine("sqlite:///tabletoolz/databases/Car_Database.db")

Base = automap_base()
Base.prepare(engine, reflect=True)

# Tables from Car database
brands = Base.classes.Brands
car_options = Base.classes.Car_Options
car_vins = Base.classes.Car_Vins
customer_ownership = Base.classes.Customer_Ownership
dealers = Base.classes.Dealers
manufacture_plant = Base.classes.Manufacture_Plant
models = Base.classes.Models


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'tabletoolz.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output
