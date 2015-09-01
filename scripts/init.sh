#!/bin/bash

# Read database configuration
USERNAME=`cat db-config.php | grep username | cut -d \' -f 4`
PASSWORD=`cat db-config.php | grep password | cut -d \' -f 4`

# Create database
mysql --local-infile --user=$USERNAME --password=$PASSWORD < scripts/create_database.sql

# Create datatables
vendor/bin/doctrine orm:schema-tool:update --force --dump-sql

# Unzip GTFS files to temporary directory
GTFS_PATH=$1

rm -rf /tmp/gtfs2arrdep
unzip $GTFS_PATH -d /tmp/gtfs2arrdep

# Load data into tables
## agency.txt
php scripts/load_agency.php /tmp/gtfs2arrdep/agency.txt
