# phaser-example
Example of project that uses the phaser library to collect, merge and transform data

## Contributing

Set up (using Python 3)

    * python3 -m venv venv
    * source venv/bin/activate
    * pip install -r requirements.txt
    # expects phaser to be installed in a parallel directory. If that is not
    # where you installed phaser, then point to where you have phaser located
    # locally
    * pip install -e ../phaser 


## Data

Desired data and rough table layout

```
counters
 - id: internal primary key
 - ext_id: external 3rd party identifier of the location of the counter
 - name: descriptive name of the location of the counter
 - municipality: where the counter resides
 - latitutde
 - longitude
 - timezone: full text of the timezone like (America/New_York)

counts
 - counter_id: foreign key to the counter id
 - counted_at: UTC timestamp when the count was observed
 - count: the number of bikes that were counted at that time
