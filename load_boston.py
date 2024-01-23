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
            last_row["count"] += row["count"]
        else:
            new_batch.append(row)
    return new_batch

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
            IntColumn("count", rename=["CNT_TOTAL"]),
            Column("municipality"),
            Column("description", rename=["CNT_LOC_DESCRIPTION"]),
            ]
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
