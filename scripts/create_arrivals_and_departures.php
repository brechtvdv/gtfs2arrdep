<?php

require_once "bootstrap.php";

use Ivory\JsonBuilder\JsonBuilder;

date_default_timezone_set('UTC');

$date_serviceId_pairs = [];

// Let's generate list of dates with corresponding serviceId of days that drive
// From calendars
$sql = "
        SELECT *
          FROM calendars
    ";

$stmt = $entityManager->getConnection()->prepare($sql);
$stmt->execute();
$calendars = $stmt->fetchAll();

for ($i = 0; $i < count($calendars); $i++) {
    $calendar = $calendars[$i];

    $startDate = $calendar['startDate'];
    $endDate = $calendar['endDate'];

    // loop all days between start_date and end_date
    for ($date = strtotime($startDate); $date < strtotime($endDate); $date = strtotime('+1 day', $date)) {
        // check if the day on this date is valid drive day
        // we use dayOfWeek as offset in calendar array
        $dayOfWeekNum = date('N',$date);
        $day = getDayFromNum($dayOfWeekNum);
        if ($calendar[$day] == '1') {
            // add to pairs
            $arrdepdate = date('Y-m-d', $date);
            $service = $calendar['serviceId'];
            $date_serviceId_pairs[] = [$arrdepdate, $service];
        }
    }
}

function getDayFromNum($dayOfWeekNum) {
    switch ($dayOfWeekNum) {
        case 1:
            return 'monday';
        case 2:
            return 'tuesday';
        case 3:
            return 'wednesday';
        case 4:
            return 'thursday';
        case 5:
            return 'friday';
        case 6:
            return 'saturday';
        case 7:
            return 'sunday';
    }
}

// Now parse calendar_dates
// When exceptionType equals 1 -> add to date_serviceId_pairs
// When exceptionType equals 2 -> remove

$sql = "
        SELECT *
          FROM calendarDates
    ";

$stmt = $entityManager->getConnection()->prepare($sql);
$stmt->execute();
$calendarDates = $stmt->fetchAll();

for ($i = 0; $i < count($calendarDates); $i++) {
    $calendarDate = $calendarDates[$i];

    if ($calendarDate['exceptionType'] == "1") {
        $date_serviceId_pairs[] = [$calendarDate['date'], $calendarDate['serviceId']];
    } else {
        $date_serviceId_pairs = removeDateServiceIdPairFromPairs([$calendarDate['date'], $calendarDate['serviceId']], $date_serviceId_pairs);
    }
}

function removeDateServiceIdPairFromPairs($dateServiceIdPair, $pairs) {
    $pairsWithoutException = [];

    for ($j = 0; $j < count($pairs); $j++) {
        if ($dateServiceIdPair != $pairs[$j]) {
            $pairsWithoutException[] = $pairs[$j];
        } else {
            // exception gets deleted
        }
    }

    return $pairsWithoutException;
}

// Now our pairs are a merge of calendars and calendar_dates
// Time for generating departures and arrivals

generateArrivals($date_serviceId_pairs, $entityManager);

function generateArrivals($date_serviceId_pairs, $entityManager)
{
    //$arrivals = []; // all the information we want to keep goes in here
    $builder = new JsonBuilder();
    $builder->setValues(array());
    $k = 0; // path to append data

    // Loop through all dates
    for ($i = 0; $i < count($date_serviceId_pairs); $i++) {
        $date = $date_serviceId_pairs[$i][0];
        $serviceId = $date_serviceId_pairs[$i][1];

        $sql = "
            SELECT *
              FROM trips
              WHERE serviceId = ?
        ";

        $stmt = $entityManager->getConnection()->prepare($sql);
        $stmt->bindValue(1, $serviceId);
        $stmt->execute();
        $trips = $stmt->fetchAll();

        $tripRouteIdPair = [];
        $tripMatches = [];
        for ($j = 0; $j < count($trips); $j++) {
            $tripMatches[] = "'" . $trips[$j]['tripId'] . "'";
            $tripRouteIdPair[] = [$trips[$j]['tripId'], $trips[$j]['routeId']];
        }

        $tripMatches = join(' , ', $tripMatches);
        $sql = "
            SELECT *
              FROM stoptimes
                JOIN trips
                  ON trips.tripId = stoptimes.tripId
                JOIN stops
                  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $tripMatches )
              ORDER BY stoptimes.arrivalTime ASC;
        ";

        $stmt = $entityManager->getConnection()->prepare($sql);
        $stmt->execute();
        $arrivalsArray = $stmt->fetchAll();

        for ($z = 0; $z < count($arrivalsArray); $z++) {
            $arrivalData = $arrivalsArray[$z];

            $arrival = [
                '@type'             => 'Arrival',
                'date'              => $date,
                'gtfs:arrivalTime'  => substr($arrivalData['arrivalTime'], 0, 5), // we only need hh:mm
                'gtfs:stop'         => $arrivalData['stopId'],
                'gtfs:trip'         => $arrivalData['tripId'],
                'gtfs:route'        => findRouteId($arrivalData['tripId'], $tripRouteIdPair)
            ];

            $builder->setValue("[$k]", $arrival);
            $k++;
        }
    }

    $json = $builder->build();
    var_dump($json);
}

function findRouteId($tripId, $tripRouteIdPair) {
    for ($i = 0; $i < count($tripRouteIdPair); $i++) {
        if ($tripRouteIdPair[$i][0] == $tripId) {
            return $tripRouteIdPair[$i][1];
        }
    }
}