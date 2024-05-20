from datetime import datetime
import phaser


# The names of the columns that contain sensor counts
ts_names = ['CNT_0630', 'CNT_0645', 'CNT_0700', 'CNT_0715', 'CNT_0730', 'CNT_0745',
            'CNT_0800', 'CNT_0815', 'CNT_0830', 'CNT_0845', 'CNT_0900', 'CNT_0915',
            'CNT_0930', 'CNT_0945', 'CNT_1000', 'CNT_1015', 'CNT_1030', 'CNT_1045',
            'CNT_1100', 'CNT_1115', 'CNT_1130', 'CNT_1145', 'CNT_1200', 'CNT_1215',
            'CNT_1230', 'CNT_1245', 'CNT_1300', 'CNT_1315', 'CNT_1330', 'CNT_1345',
            'CNT_1400', 'CNT_1415', 'CNT_1430', 'CNT_1445', 'CNT_1500', 'CNT_1515',
            'CNT_1530', 'CNT_1545', 'CNT_1600', 'CNT_1615', 'CNT_1630', 'CNT_1645',
            'CNT_1700', 'CNT_1715', 'CNT_1730', 'CNT_1745', 'CNT_1800', 'CNT_1815',
            'CNT_1830', 'CNT_1845', 'CNT_1900', 'CNT_1915', 'CNT_1930', 'CNT_1945',
            'CNT_2000', 'CNT_2015', 'CNT_2030', 'CNT_2045']


ts_columns = [phaser.IntColumn(name) for name in ts_names]


COLUMNS = [
      phaser.Column("location_id", rename=["BP_LOC_ID"]),
      phaser.Column("latitude"),
      phaser.Column("longitude"),
      phaser.Column("count_id"),
      # Count type values:
      # - A: All (or unspecified)
      # - B: Bicycle
      # - C: Baby carriage
      # - J: Jogger
      # - O: Other
      # - P: Pedestrian
      # - S: Skater, Rollerblader
      # - W: Wheelchair
      phaser.Column("COUNT_TYPE",
                    allowed_values=["A", "B", "C", "J", "O", "P", "S", "W"],
                    on_error=phaser.ON_ERROR_DROP_ROW),
      phaser.DateColumn("count_date"),
      phaser.Column("municipality"),
      phaser.Column("description", rename=["CNT_LOC_DESCRIPTION"]),
      phaser.IntColumn("temperature")
  ] + ts_columns


@phaser.row_step
def keep_only_declared_columns(row, **kwargs):
    return {c.name: row[c.name] for c in COLUMNS}


@phaser.dataframe_step
def sum_counts(df, context):
    def agg_fn(col_name):
        if col_name.startswith('CNT_'):
            return 'sum'
        return 'first'

    aggregate_instructions = {column: agg_fn(column) for column in df.columns if column != 'count_id'}
    return df.groupby(['count_id']).agg(aggregate_instructions).reset_index()

@phaser.batch_step
def pivot_timestamps(batch, context):
    """
    Turns each individual count throughout the day into its own row with
    a properly constructed timestamp column
    """
    new_batch = []
    for row in batch:
        for ts_column in ts_names:
            # Throw out rows that have no recorded count at the timestamp,
            # only keeping pivoted rows that have valid data.
            if row[ts_column] != None:
                new_row = copy_common_columns(row)
                new_row["count"] = row[ts_column]
                new_row["counted_at"] = make_ts(row["count_date"], ts_column)
                new_batch.append(new_row)
    return new_batch


def copy_common_columns(row):
    """
    Makes a copy of the data from row for all the columns that are to be kept
    for each new row that we make when pivoting to a timestamped set of data.
    """
    columns = [
        "location_id",
        "latitude",
        "longitude",
        "count_id",
        "municipality",
        "description",
        "temperature"
    ]
    return {column: row[column] for column in columns}


def make_ts(date, column):
    """
    Construct a datetime from the date information and the name of the column.
    `date` is datetime.date.
    `column` is a string that looks like this: "CNT_0830" where the last four
    digits represent a clock time of HHMM
    """
    time = column.split("_")[-1]
    hour = int(time[:2])
    minute = int(time[2:])
    return datetime(date.year, date.month, date.day, hour, minute)


class BostonPipeline(phaser.Pipeline):

    phases = [
        phaser.Phase(name="select-bike-counts",
                     steps=[
                         phaser.filter_rows(lambda row: row['COUNT_TYPE'] == 'B'),
                         keep_only_declared_columns,
                     ],
                     columns=COLUMNS,
                     ),
        phaser.Phase(name="aggregate-counts",
                     steps=[phaser.sort_by('count_id'), sum_counts],
                     columns=COLUMNS),
        phaser.Phase(name="pivot-timestamps",
                     steps=[pivot_timestamps],
                     columns=COLUMNS),
    ]
