<?php

require_once "bootstrap.php";

date_default_timezone_set('UTC');

$file_stop_times = $argv[1];
$batchSize = 30; // stop_times.txt is very big, so we flush regularly

if (($handleRead = fopen($file_stop_times, 'r')) !== false) {
    // header
    // important to know which column data is provided
    $header = fgetcsv($handleRead);
    $column_count = count($header);

    $row = 1; // row_counter
    // loop through the file line-by-line
    while (($line = fgetcsv($handleRead)) !== false) {
        $stoptime = new Stoptime();

        for ($i = 0; $i < $column_count; $i++) {
            save($stoptime, $header[$i], $line[$i]);
        }

        unset($line);

        $entityManager->persist($stoptime);
        if (($row % $batchSize) === 0) {
            $entityManager->flush();
            $entityManager->clear(); // Detaches all objects from Doctrine
        }

        $row++;
    }

    fclose($handleRead);

    $entityManager->flush();

    echo "Created " . $row . " stoptimes" . "\n";
} else {
    echo "Something went wrong with reading stop_times.txt" . "\n";
}

function save($stoptime, $property, $value) {
    switch ($property) {
        case 'trip_id':
            $stoptime->setTripId($value);
            break;
        case 'arrival_time':
            $arrival_time = \DateTime::createFromFormat('H:i:s', $value);
            $stoptime->setArrivalTime($arrival_time);
            break;
        case 'departure_time':
            $departure_time = \DateTime::createFromFormat('H:i:s', $value);
            $stoptime->setDepartureTime($departure_time);
            break;
        case 'stop_id':
            $stoptime->setStopId($value);
            break;
        case 'stop_sequence':
            $stoptime->setStopSequence($value);
            break;
        case 'stop_headsign':
            $stoptime->setStopHeadsign($value);
            break;
        case 'pickup_type':
            $stoptime->setPickupType($value);
            break;
        case 'drop_off_type':
            $stoptime->setDropOffType($value);
            break;
        case 'shape_dist_traveled':
            $stoptime->setShapeDistTraveled($value);
            break;
        case 'timepoint':
            $stoptime->setTimepoint($value);
            break;
    }
}