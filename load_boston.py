from phaser import Pipeline, Phase, Column, DateColumn, IntColumn, batch_step, row_step

@row_step
def echo_step(row, **kwargs):
    print(row)
    return row

def select(fn):
    @row_step
    def _select(row, **kwargs):
        if fn(row) == True:
            return row
        return None
    return _select

@row_step
def keep_only_declared_columns(row, context):
    columns = context["columns"]
    new_row = {c.name: row[c.name] for c in columns}
    return new_row

@row_step
def throw_out_zero_totals(row, context):
    if row["count"] > 0:
        return row
    return None

@batch_step
def sum_counts(batch, context):
    new_batch = [batch[0]]
    # Assume the data is sorted by count_id, since that is what we are
    # aggregating by.
    # TODO: Add in a step to sort?
    for row in batch[1:]:
        last_row = new_batch[-1]
        if row["count_id"] == last_row["count_id"]:
            add_in_counts(last_row, row)
        else:
            new_batch.append(row)
    return new_batch

def add_in_counts(agg_row, row):
    """Add the counts from `row` into the same columns in the `agg_row`"""
    agg_row["count"] += row["count"]
    for column in ts_names:
        agg_row[column] += row[column]

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
ts_columns = [IntColumn(name, required=False, default=0) for name in ts_names]

class BostonPipeline(Pipeline):
    columns = [
            Column("location_id", rename=["BP_LOC_ID"]),
            Column("latitude"),
            Column("longitude"),
            Column("count_id"),
            # Count type values:
            # - A: All (or unspecified)
            # - B: Bicycle
            # - C: Baby carriage
            # - J: Jogger
            # - O: Other
            # - P: Pedestrian
            # - S: Skater, Rollerblader
            # - W: Wheelchair
            Column("COUNT_TYPE", allowed_values=["A","B","C","J","O","P","S","W"]),
            DateColumn("count_date"),
            Column("municipality"),
            Column("description", rename=["CNT_LOC_DESCRIPTION"]),
            IntColumn("count", rename=["CNT_TOTAL"]),
            ] + ts_columns
    phases = [
            Phase(name="select-bike-counts",
                  steps=[
                      select(lambda x: x["COUNT_TYPE"] == "B"),
                      keep_only_declared_columns,
                      throw_out_zero_totals,
                      ],
                  columns=columns,
                  context={
                      "columns": columns,
                      }
                  ),
            Phase(name="aggregate-counts",
                  steps=[
                      sum_counts,
                      ],
                  columns=columns,
                  ),
            ]

    def __init__(self, working_dir=None, source=None):
        super().__init__(working_dir, source, self.__class__.phases)


p = BostonPipeline("/tmp/phaser-example", "sources/boston/bike_ped_counts.csv")
p.run()
