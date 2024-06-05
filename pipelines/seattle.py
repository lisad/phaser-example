import phaser

"""
Seattle pipeline wishlist
* A builtin method to keep only declared columns
* How do we handle columns with non fixed names? the sum_cyclist_values -- has to do its own casting rather than
be able to define a IntColumn
* Should we ensure columns stay in order in dataframe steps?  Originally I tried to get
the location_name from df.columns[1] in a dataframe step, but it was not in source file order
* Should there be a column header type step - a step that just gets the column header names and returns a new set?
see how awkward it is to get the 2nd column name in a batch step
"""


@phaser.batch_step
def get_location_name(batch, context):
    context.add_variable('location_name', list(batch[0].keys())[1])
    return batch


@phaser.dataframe_step
def add_location_values_to_rows(df, context):
    # In the seattle data, the location of the sensors can be found in the 2nd column name (that has the totals
    # of both pedestrian and cyclist counts) but we want that to be a column value for aggregating with other
    # locations.
    location_name = context.get('location_name')
    df['municipality'] = "Seattle"
    df['description'] = location_name
    return df


@phaser.row_step
def sum_cyclist_values(row, context):
    cyclist_sum = sum( int(value) for key, value in row.items() if "Cyclist" in key and value != '')
    row['count'] = cyclist_sum
    return row


@phaser.row_step
def keep_only_declared_columns(row, **kwargs):
    return {name: row[name] for name in ['counted_at', 'municipality', 'description', 'count']}


class SeattlePipeline(phaser.Pipeline):
    phases = [
        phaser.Phase(columns=[phaser.DateTimeColumn('counted_at', rename='Time')],
                     steps=[get_location_name,
                            add_location_values_to_rows,
                            sum_cyclist_values,
                            keep_only_declared_columns
                    ]
        )
    ]
