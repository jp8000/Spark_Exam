
from Spark_Exam.src.spark import parse_address
import sys
import os

# Add the path to the root directory of your project
sys.path.append(os.path.abspath("Spark_Exam"))


def test_parse_address():
    address = "123, Test St, Test City, Test Country"
    parsed_address = parse_address(address)
    
    assert parsed_address == {
        'streetNumber': '123',
        'streetName': 'Test St',
        'city': 'Test City',
        'country': 'Test Country'
    }


# when 4 values an not recieved
def test_parse_address_missing_comma():
    address = "123 Test St Test City Test Country"
    parsed_address = parse_address(address)
    
    assert parsed_address == {
        'streetNumber': '',
        'streetName': '',
        'city': '',
        'country': ''
    }




# command: python3 -m pytest -v test_functions.py
