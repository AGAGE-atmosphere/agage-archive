import pytest
import numpy as np

from agage_archive.definitions import get_instrument_type, \
    get_instrument_number, define_instrument_number, \
    instrument_type_definition


def test_define_instrument_number():
    '''Test define_instrument_number function'''

    network = "agage_test"

    # Test for valid network
    instrument_number = define_instrument_number(network)
    assert isinstance(instrument_number, dict)
    assert "UNDEFINED" in instrument_number
    assert instrument_number["UNDEFINED"] == -1
    assert len(instrument_number) > 1
    for key, value in instrument_number.items():
        assert isinstance(key, str)
        assert isinstance(value, int)


def test_instrument_type_definition():
    '''Test instrument_type_definition function'''

    network = "agage_test"

    # Test for valid network
    instrument_number, instrument_number_string = instrument_type_definition(network)
    assert isinstance(instrument_number, dict)
    assert isinstance(instrument_number_string, str)
    for key, value in instrument_number.items():
        assert key in instrument_number_string
        assert str(value) in instrument_number_string
    
    # Check that string is the right length from number of commas
    assert instrument_number_string.count(",") == len(instrument_number) - 1


def test_get_instrument_type():
    '''Test get_instrument_type function'''
    
    network = "agage_test"

    # Test for single instrument number
    instrument_number = define_instrument_number(network)

    for instrument, number in instrument_number.items():
        instrument_type = get_instrument_type(number, network)
        assert isinstance(instrument_type, str)
        assert instrument_type == instrument


def test_get_instrument_number():
    '''Test get_instrument_number function'''

    network = "agage_test"

    instrument_number = define_instrument_number(network)

    for instrument, number in instrument_number.items():
        instrument_num = get_instrument_number(instrument, network)
        assert isinstance(instrument_num, int)
        assert instrument_num == number

    # Check for partial match
    instrument_num = get_instrument_number("Picarro-1", network)
    assert isinstance(instrument_num, int)
    assert instrument_num == instrument_number["Picarro"]
