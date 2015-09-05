# gtfs2arrdep
Transforms GTFS into arrival/departures in JSON format.

This project is written in PHP and uses [doctrine](http://www.doctrine-project.org/) to load the GTFS files into a MySQL database.

Explanation of the data model of arrival and departure objects can be found [here](https://github.com/linkedconnections/arrdep2connections#1-arrival-objects-and-departure-objects).

## Requirements

    * Composer
    * MySQL
    * PHP 5.4+

## Install

We use the PHP package manager [Composer](http://getcomposer.org). Make sure it's installed and then run from this directory:

```bash
composer install
```

## Generating arrivals/departures

### Step 1: Setup database configuration

Fill in your MySQL credentials inside ```db-config.php```.

### Step 2: Run database load script

```bash
scripts/init.sh path-to-gtfs.zip
```

### Step 3: Run arrivals/departures generator script

```bash
php scripts/create_arrivals_and_departures.php [startDate] [endDate]
```

Parameters ```startDate``` and ```endDate``` currently only work when there's only a calendar_dates.txt file.
The format of date must be 'YYYY-MM-DD'.

### Step 4: Done

You can find arrivals-[agency_id].json and departures-[agency_id].json in ```dist``` folder.
