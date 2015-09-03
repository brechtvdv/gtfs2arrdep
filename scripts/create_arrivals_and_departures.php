<?php

require_once "bootstrap.php";

use Ivory\JsonBuilder\JsonBuilder;

date_default_timezone_set('UTC');

$date_serviceIds = [];

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
            addDateServiceId($date_serviceIds, $arrdepdate, $service);
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

// get agencyId for output filenames
$sql = "
        SELECT *
          FROM agency
    ";

$stmt = $entityManager->getConnection()->prepare($sql);
$stmt->execute();
$agencyArray = $stmt->fetchAll();
$arrivalsFilename = 'dist/arrivals-' . $agencyArray[0]['agencyId'] . '.json';
$departuresFilename = 'dist/departures-' . $agencyArray[0]['agencyId'] . '.json';

// delete previous files
unlink($arrivalsFilename);
unlink($departuresFilename);

// Now parse calendar_dates
if (count($calendars) > 0) {
    $sql = "
        SELECT *
          FROM calendarDates
    ";
    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    $calendarDates = $stmt->fetchAll();

    // Hopefully no memory problems
    $date_serviceIdsArray = addCalendarDates($date_serviceIds, $calendarDates);

    // We have now merged calendars and calendar_dates
    // Time for generating departures and arrivals
    generateArrivalsDepartures($date_serviceIdsArray, $entityManager);
} else {
    // There can be a lot calendar_dates in this case
    // That's why we'll get start_date and end_date from feed_info
    // Then we'll fetch all serviceIds for every day
    $sql = "
            SELECT MIN(date) startDate, MAX(date) endDate
              FROM calendarDates
        ";
    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    $startAndEndDate = $stmt->fetchAll();

    $startDate = $startAndEndDate[0]['startDate'];
    $endDate = $startAndEndDate[0]['endDate'];

    // loop all days between start_date and end_date
    for ($date = strtotime($startDate); $date < strtotime($endDate); $date = strtotime('+1 day', $date)) {
        $sql = "
            SELECT *
              FROM calendarDates
              WHERE date = ?
        ";
        $stmt = $entityManager->getConnection()->prepare($sql);
        $d = date('Y-m-d', $date);
        $stmt->bindParam(1, $d);
        $stmt->execute();
        $calendarDates = $stmt->fetchAll();

        $date_serviceIdsArray = addCalendarDates($date_serviceIds, $calendarDates);

        generateArrivalsDepartures($date_serviceIdsArray, $entityManager);

        $date_serviceIds = [];
        $date_serviceIdsArray = [];
    }
}

function addDateServiceId($data_serviceIdsArray, $date, $serviceId) {
    if (!isset($data_serviceIdsArray[$date])) {
        $data_serviceIdsArray[$date] = [];
    }

    $data_serviceIdsArray[$date][] = $serviceId;
    return $data_serviceIdsArray;
}

function removeDateServiceId($data_serviceIdsArray, $date, $serviceId) {
    $date_serviceIdsWithoutException = [];

    for ($j = 0; $j < count($data_serviceIdsArray); $j++) {
        if ($data_serviceIdsArray[$j][0] == $date) {
            for ($k = 0; $k < count($data_serviceIdsArray[$j][0]); $k++) {
                if ($data_serviceIdsArray[$j][0][$k] != $serviceId) {
                    $date_serviceIdsWithoutException[$j][0][] = $data_serviceIdsArray[$j][0][$k];
                } else {
                    // ignore exception
                }
            }
        } else {
            $date_serviceIdsWithoutException[] = $data_serviceIdsArray[$j][0];
        }
    }

    return $date_serviceIdsWithoutException;
}

function addCalendarDates($data_serviceIdsArray, $calendarDates) {
    for ($i = 0; $i < count($calendarDates); $i++) {
        $calendarDate = $calendarDates[$i];

        // When exceptionType equals 1 -> add to date_serviceId_pairs
        // When exceptionType equals 2 -> remove
        if ($calendarDate['exceptionType'] == "1") {
            $data_serviceIdsArray = addDateServiceId($data_serviceIdsArray, $calendarDate['date'], $calendarDate['serviceId']);
        } else {
            $data_serviceIdsArray = removeDateServiceId($data_serviceIdsArray, $calendarDate['date'], $calendarDate['serviceId']);
        }
    }
    return $data_serviceIdsArray;
}

/**
 * @param $date_serviceIdsArray
 * @param $entityManager
 */
function generateArrivalsDepartures($date_serviceIdsArray, $entityManager)
{
    global $arrivalsFilename, $departuresFilename;

    // Loop through all dates
    for ($i = 0; $i < count($date_serviceIdsArray); $i++) {
        $date = array_keys($date_serviceIdsArray)[0];
        $serviceIds = array_shift($date_serviceIdsArray);

        $serviceMatches = [];
        for ($j = 0; $j < count($serviceIds); $j++) {
            $serviceMatches[] = "'" . $serviceIds[$j] . "'";
        }
        $serviceMatches = join(' , ', $serviceMatches);

        $sql = "
            SELECT *
              FROM trips
              WHERE serviceId IN ( $serviceMatches )
        ";

        $stmt = $entityManager->getConnection()->prepare($sql);
        $stmt->execute();
        $trips = $stmt->fetchAll();

        $tripRouteIdPair = [];
        $tripMatches = [];
        for ($j = 0; $j < count($trips); $j++) {
            $tripMatches[] = "'" . $trips[$j]['tripId'] . "'";
            $tripRouteIdPair[] = [$trips[$j]['tripId'], $trips[$j]['routeId']];
        }

        $tripMatches = join(' , ', $tripMatches);

        // ARRIVALS
        $arrivalsArray = queryArrivals($entityManager, $tripMatches);

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

            writeToFile($arrivalsFilename, $arrival);
        }

        unset($arrivalsArray); // free memory

        // DEPARTURES
        $departuresArray = queryDepartures($entityManager, $tripMatches);

        for ($z = 0; $z < count($departuresArray); $z++) {
            $departureData = $departuresArray[$z];

            $departure = [
                '@type'             => 'Departure',
                'date'              => $date,
                'gtfs:departureTime'  => substr($departureData['departureTime'], 0, 5), // we only need hh:mm
                'gtfs:stop'         => $departureData['stopId'],
                'gtfs:trip'         => $departureData['tripId'],
                'gtfs:route'        => findRouteId($departureData['tripId'], $tripRouteIdPair)
            ];

            writeToFile($departuresFilename, $departure);
        }

        unset($departuresArray); // free memory
    }
}

function queryArrivals($entityManager, $trips) {
    $sql = "
            SELECT *
              FROM stoptimes
                JOIN trips
                  ON trips.tripId = stoptimes.tripId
                JOIN stops
                  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $trips )
              ORDER BY stoptimes.arrivalTime ASC;
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

function queryDepartures($entityManager, $trips) {
    $sql = "
            SELECT *
              FROM stoptimes
                JOIN trips
                  ON trips.tripId = stoptimes.tripId
                JOIN stops
                  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $trips )
              ORDER BY stoptimes.departureTime ASC;
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

function findRouteId($tripId, $tripRouteIdPair) {
    for ($i = 0; $i < count($tripRouteIdPair); $i++) {
        if ($tripRouteIdPair[$i][0] == $tripId) {
            return $tripRouteIdPair[$i][1];
        }
    }
}

function writeToFile($filename, $data) {
    $builder = new JsonBuilder();
    $builder->setValues($data);
    $json = $builder->build();

    // write new one
    file_put_contents($filename, $json.PHP_EOL, FILE_APPEND);
}