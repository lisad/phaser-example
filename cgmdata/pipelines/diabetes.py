
import phaser
import pandas
from datetime import datetime

class ExtractPhase(phaser.Phase):
    columns = [
        # Columns to drop
        phaser.Column('units', required=False, save=False),
        phaser.Column('deviceId', save=False),
        phaser.Column('uploadId', save=False),
        phaser.Column('suppressed', save=False),
        phaser.Column('expectedNormal', required=False, save=False),
            # Should document why some columns need 'required=False' even with 'save=False' -
            # columns may not appear in some rows. Should we not error if a column is missing
            # if save=False?
        phaser.Column('id', save=False),
        phaser.Column('guid', save=False),
        phaser.Column('clockDriftOffset', save=False),
        phaser.Column('conversionOffset', save=False),
        phaser.Column('payload__timestamp', required=False, save=False),
        phaser.Column('payload__logIndices', required=False, save=False),
        phaser.Column('payload__timestamp', required=False, save=False)
        # This phase illustrates why we want issue #187, maybe work on it
    ]

    steps = [
        phaser.filter_rows(lambda row: row['type'] in ['cbg', 'basal']),
        phaser.flatten_column('payload'),
        phaser.filter_rows(lambda row: keep_basal_and_fmr(row))
    ]


def keep_basal_and_fmr(row):
    return (row['type'] == 'basal'
            or row.get('payload__type') == ['Five Minute Reading (FMR)'])


@phaser.row_step
def set_hour_and_date(row, context):
    row['date'] = row['time'].date()
    row['hour'] = row['time'].hour
    return row


@phaser.dataframe_step(pass_row_nums=False)
def calculate_hour_avg(df, context):
    averages = df.groupby(['date', 'hour'], as_index=False).agg({'cbg_value': 'mean', 'basal_rate': 'mean'})
    return averages


class CalculationPhase(phaser.Phase):
    columns = [
        phaser.DateTimeColumn('time', required=True),
        phaser.FloatColumn('basal_rate', rename='rate'),
        phaser.FloatColumn('cbg_value', rename='value')
    ]

    steps = [
        set_hour_and_date,
        calculate_hour_avg
    ]


class GlucosePipeline(phaser.Pipeline):
    # save_format = phaser.JSON_RECORD_FORMAT
    phases = [ExtractPhase, CalculationPhase]
