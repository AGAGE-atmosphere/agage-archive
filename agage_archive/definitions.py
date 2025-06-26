import numpy as np

nc4_types = {"f4": "float32",
            "f8": "float64",
            "i4": "int32",
            "i2": "int16",
            "i8": "int64",
            "i1": "int8",
            "u1": "uint8",
            "u2": "uint16",
            "u4": "uint32",
            "u8": "uint64",
            "S1": "S1"}

scale_translator = {"TU1987": "TU-87"}

unit_translator = {"ppm": "1e-6",
                    "ppb": "1e-9",
                    "ppt": "1e-12",
                    "ppq": "1e-15",
                    "nmol/mol": "1e-9",
                    "nmol mol-1": "1e-9",
                    "pmol/mol": "1e-12",
                    "pmol mol-1": "1e-12",
                    }

species_translator = {"pfc-116": "c2f6",
                      "pfc-218": "c3f8",
                      "pfc-318": "c4f8",
                      "pce": "ccl2ccl2",
                      "tce": "chclccl2",
                      "benzene": "c6h6",
                      "propane": "c3h8",
                      "ethane": "c2h6",
                      "ethyne": "c2h2",
                      "c-propane": "c3h6",
                      "toluene": "c6h5ch3",
                      }

species_translator_flask = {"c2f6": "PFC-116",
                            "c3f8": "PFC-218",
                            "c4f8": "PFC-318",
                            "c6h6": "benzene",
                            "hfc-134a": "HFC-134a",
                            "hfc-152a": "HFC-152a",
                            "hfc-143a": "HFC-143a",
                            "hfc-227ea": "HFC-227ea",
                            "hfc-236fa": "HFC-236fa",
                            "hfc-245fa": "HFC-245fa",
                            "hfc-365mfc": "HFC-365mfc",
                            "hfc-4310mee": "HFC-4310mee",
                            "hcfc-22": "HCFC-22",
                            "hcfc-141b": "HCFC-141b",
                            "hcfc-142b": "HCFC-142b",
                            "hcfc-132b": "HCFC-132b",
                            "hcfc-133a": "HCFC-133a",
                            "ch3cl": "CH3Cl",
                            "ch3br": "CH3Br",
                            "ch2cl2": "CH2Cl2",
                            "chcl3": "CHCl3",
                            "ch3ccl3": "CH3CCl3",
                            "ccl4": "CCl4",
                            "ccl2ccl2": "PCE",
                            "chclccl2": "TCE",
                            }

instrument_number = {"UNDEFINED": -1,
                    "ALE": 0,
                    "GAGE": 1,
                    "GCMD": 2,
                    "GCMS-ADS": 3,
                    "GCMS-Medusa": 4,
                    "GCMS-Medusa-flask": 5,
                    "GCECD": 6,
                    "GCTOFMS": 7,
                    "Picarro": 8,
                    "LGR": 9,
                    "GCMS-MteCimone": 10,
                    "GCPDD": 11,
                    "GCMS-Magnum": 12,
                    "GCMS": 13,}

minimum_averaging_period = {"Picarro": "1H"}

instrument_selection_text = "Recommended instrument(s) selected and combined by AGAGE PIs"


def instrument_type_definition():
    '''Define instrument numbers for each instrument type

    Returns:
        str: Instrument type definition
    '''
    
    # Create string from dictionary defining instrument numbers
    instrument_number_string = ", ".join([f"{k}={v}" for k, v in instrument_number.items()])

    return instrument_number, instrument_number_string


def get_instrument_type(instrument_numbers):
    '''Get instrument type name from instrument number

    Args:
        instrument_number (int or list): Instrument number(s)

    Returns:
        str or list: Instrument type name(s)
    '''

    # If instrument_numbers is an int, return a single string
    if isinstance(instrument_numbers, (int, np.integer)) and instrument_numbers in instrument_number.values():
        instrument_type = [k for k, v in instrument_number.items() if v == instrument_numbers][0]
    # If instrument_numbers is a list, return a list of strings
    elif isinstance(instrument_numbers, list):
        instrument_type = [k for k, v in instrument_number.items() if v in instrument_numbers]
    else:
        raise ValueError("instrument_numbers must be an int or list")

    return instrument_type


def get_instrument_number(instrument):
    '''Get instrument number from instrument type name

    Args:
        instrument (str): Instrument type name

    Returns:
        int: Instrument number
    '''

    if isinstance(instrument, (int, np.integer)):
        raise ValueError("instrument cannot be an int")

    if len(instrument) <= 1:
        raise ValueError("instrument cannot be a single character or empty string")

    instrument_type = -999

    if instrument in instrument_number:
            # First try to find an exact match
        instrument_type = instrument_number[instrument]
    else:
        # If not, try to find a partial match (e.g., Picarro-1 -> Picarro)
        for k, v in instrument_number.items():
            if k in instrument:
                instrument_type = v
                break
    
    # If instrument_type hasn't been defined, raise an error
    if instrument_type == -999:
        raise KeyError(f"Could not find instrument number for {instrument}")
    
    return instrument_type

