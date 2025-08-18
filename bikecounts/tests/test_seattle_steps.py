import pytest

from pipelines.seattle import sum_cyclist_values

def test_sum_cyclist_values_bgt_data():
    data = {'Bike North': 1, 'Bike South': 1, 'Ped North': 10, 'Ped South': 10}
    assert sum_cyclist_values(data)['count'] == 2

def test_sum_cyclist_values_tso_data():
    data = {
        'Thomas St Overpass Pedestrian IN': 10,
        'Thomas St Overpass Pedestrian OUT': 10,
        'Thomas St Overpass Cyclist IN': 1,
        'Thomas St Overpass Cyclist OUT': 1}
    assert sum_cyclist_values(data)['count'] == 2

def test_sum_cyclist_values_empty_values():
    data = { 'Bike east': '', 'Bike west': 5  }
    assert sum_cyclist_values(data)['count'] == 5