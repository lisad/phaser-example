
import pytest
import pandas as pd
from diabetes import calculate_hour_avg


def test_calculate_averages_aggregated():
    data = {
        'hour': ['1', '1', '2', '2'],
        'date': ['20240218', '20240218', '20240218', '20240218'],
        'basal_rate': [1.422, None, 0.9, None],
        'cbg_value': [None, 8.77, None, 9.43]
    }
    df = pd.DataFrame(data)
    output, _ = calculate_hour_avg(df)
    for row in output:
        if row['hour'] == '1':
            assert row['cbg_value'] == 8.77
            assert row['basal_rate'] == 1.422
