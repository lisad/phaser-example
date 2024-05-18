# phaser-example

This is a working example of project that uses the phaser library to collect, merge and transform data.

## To try yourself

Set up (using Python 3):

```

> python3 -m venv venv
> source venv/bin/activate
> pip install -r requirements.txt
> pip install phaser
> mkdir output sources

```    

If phaser is locally cloned from the library repository, install it with `pip install -e ../phaser` (or use the 
appropriate relative directory path)

Download the __'bike_ped_counts.csv'__ data from the city of Boston and put it in the _sources_ directory.

To run the pipeline:
```    
> python3 -m phaser run boston output  sources/bike_ped_counts.csv
```

## Incoming Data

The boston bike and pedestrian count data looks like this: 

* BP_LOC_ID: location id
* LATITUDE, LONGITUDE
* COUNT_ID: unique identifier for this count record which will contain many values in many columns
* MUNICIPALITY
* FACILITY_NAME
* CNT_LOC_DESCRIPTION
* CNT_DESCRIPTION
* TEMPERATURE
* SKY
* COUNT_TYPE:  "B" for bike, "P" for pedestrian, etc.
* Additional fields describing streets, directions of traffic
* COUNT_DATE
* 58 columns named after a time of day, e.g. 'CNT_0630', 'CNT_0645', 'CNT_0700' etc.  The values in
  these columns are the counts for those 15 minute periods e.g. from 6:30 to 6:45.

The challenging thing in working with this data is turning it into individual timestamped counts which would allow
better totals, graphing, etc, rather than the 58 counts per day across each row.

Before this, however, we must deal with multiple count rows for the same location: direction coming into the 
location, and direction leaving the location (e.g. northbound on Harvard St going to westbound on Beacon st).
For our analysis, we add these all up for total traffic at that location in that time period.

## Desired output of transformation

The output should have one row per count value per timestamp, with these column values all filled in:

* location_id: location ID of bike/ped counter sensor provided by city
* latitude, longitude
* count_id: ID provided by city for the day's count values? 
* description: descriptive name of the location of the counter
* municipality: civil municipality where the counter resides
* count: number of bikes counted and registered at this time
* counted_at: timestamp of the count value 

Still to add:
* Timezone

## Overview of Boston pipeline

Columns are declared once for the whole pipeline, because several phases need to apply the same column value parsing
(e.g. parsing counts as ints, and dates as dates.) THe first time the column definitions are used in the first phase,
this results in dropping some rows with invalid values.

In the __select-bike-counts__ phase which works first to eliminate all the data we don't want to work with (a good 
practice to avoid having to add code to work with data you don't even want), only rows with bike counts and
only columns we want are kept.  This is done with the __phaser__ builtin __filter_rows__ function and a custom
function to drop all columns not declared.

The __aggregate-counts__ phase adds the counts together for all the incoming and outgoing locations as those 
are all broken into separate rows with the same COUNT_ID in the source data.

Finally, the __pivot-timestamps__ phase does a wide-to-long pivot, so that each count gets its own row and timestamp, 
now ready for graphing or analysis.  TODO: the pivot-timestamps phae needs to tell the phaser library not to 
keep row numbers or warn about extra rows created, because it's doing a pivot.

The declaration of the pipeline, columns and steps is only ~35 lines, because many of the operations are performed
by the __phaser__ library (the rest of the lines in boston.py is mostly the pivot function).  The __phaser__ library
takes care of:

* Making sure the int columns like CNT_0630 are all treated as integers
* Parsing the date column
* Dropping empty rows rather than have null values
* Input and output
* Collecting a summary of what was done in an 'errors_and_warnings.txt' file - e.g. how many rows dropped with
  COUNT_TYPE other than 'B'

After running the pipeline, the output of each phase can be seen in a checkpoint to make sure each phase separately is 
doing its job.

