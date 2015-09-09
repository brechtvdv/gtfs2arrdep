<?php

/**
 * This script generates arrivals and departures of a GTFS feed that is loaded in MySQL database.
 *
 * An arrival holds the time and date of arrival of a vehicle with corresponding stop.
 * A departure holds the time and date of departure of a vehicle with corresponding stop.
 * See https://github.com/linkedconnections/arrdep2connections#data-models for better explanation.
 * Those arrivals/departures are fetched in an ascending order, converted to JSON and streamed away to output file.
 *
 */

// This is necessary for big datasets like 'De Lijn'
// We query all trips per day and order the arrivals/departures for that day
ini_set('memory_limit', '-1');

require_once "bootstrap.php";

use Ivory\JsonBuilder\JsonBuilder;

date_default_timezone_set('UTC');

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
if (file_exists($arrivalsFilename)) {
    unlink($arrivalsFilename);
}
if (file_exists($departuresFilename)) {
    unlink($departuresFilename);
}

// This holds array of dates with corresponding serviceIds
// We need serviceIds per day to query ordered arrivals/departures
$date_serviceIds = [];

// Let's generate list of dates with corresponding serviceId from calendars first
$sql = "
        SELECT *
          FROM calendars
    ";

$stmt = $entityManager->getConnection()->prepare($sql);
$stmt->execute();
$calendars = $stmt->fetchAll();

// When there are calendars
if (count($calendars) > 0) {
    // Check if start- and/or endDate is given as parameter
    // Otherwise set minimum and maximum of calendars
    if (!isset($argv[1]) || !isset($argv[2])) {
        $sql = "
            SELECT MIN(startDate) startDate, MAX(endDate) endDate
              FROM calendars
        ";
        $stmt = $entityManager->getConnection()->prepare($sql);
        $stmt->execute();
        $startAndEndDate = $stmt->fetchAll();

        $startDate_ = $startAndEndDate[0]['startDate'];
        $endDate_ = $startAndEndDate[0]['endDate'];

        if (isset($argv[1])) {
            $startDate_ = $argv[1];
        }
        if (isset($arg[2])) {
            $endDate_ = $argv[2];
        }
    } else {
        $startDate_ = $argv[1];
        $endDate_ = $argv[2];
    }

    for ($i = 0; $i < count($calendars); $i++) {
        $calendar = $calendars[$i];

        $startDate = $calendar['startDate'];
        $endDate = $calendar['endDate'];

        // When start- and endDate of calendar fall in given interval
        if ($startDate >= $startDate_ && $endDate <= $endDate_) {
            // loop all days between start_date and end_date
            for ($date = strtotime($startDate); $date <= strtotime($endDate); $date = strtotime('+1 day', $date)) {
                // check if the day on this date drives
                // we use dayOfWeek as offset in calendar array
                $dayOfWeekNum = date('N',$date);
                $day = getDayFromNum($dayOfWeekNum);
                if ($calendar[$day] == '1') {
                    // add to pairs
                    $arrdepdate = date('Y-m-d', $date);
                    $service = $calendar['serviceId'];
                    $date_serviceIds = addDateServiceId($date_serviceIds, $arrdepdate, $service);
                }
            }
        } else if ($startDate < $startDate_ && $endDate <= $endDate_) {
            // StartDate falls before given startDate_
            for ($date = strtotime($startDate_); $date <= strtotime($endDate); $date = strtotime('+1 day', $date)) {
                // check if the day on this date drives
                // we use dayOfWeek as offset in calendar array
                $dayOfWeekNum = date('N',$date);
                $day = getDayFromNum($dayOfWeekNum);
                if ($calendar[$day] == '1') {
                    // add to pairs
                    $arrdepdate = date('Y-m-d', $date);
                    $service = $calendar['serviceId'];
                    $date_serviceIds = addDateServiceId($date_serviceIds, $arrdepdate, $service);
                }
            }
        } else if ($startDate >= $startDate_ && $endDate > $endDate_) {
            // EndDate falls after given endDate_
            for ($date = strtotime($startDate); $date <= strtotime($endDate_); $date = strtotime('+1 day', $date)) {
                // check if the day on this date drives
                // we use dayOfWeek as offset in calendar array
                $dayOfWeekNum = date('N',$date);
                $day = getDayFromNum($dayOfWeekNum);
                if ($calendar[$day] == '1') {
                    // add to pairs
                    $arrdepdate = date('Y-m-d', $date);
                    $service = $calendar['serviceId'];
                    $date_serviceIds = addDateServiceId($date_serviceIds, $arrdepdate, $service);
                }
            }
        } else if ($startDate < $startDate_ && $endDate > $endDate_) {
            // Both overlap interval
            for ($date = strtotime($startDate_); $date <= strtotime($endDate_); $date = strtotime('+1 day', $date)) {
                // check if the day on this date drives
                // we use dayOfWeek as offset in calendar array
                $dayOfWeekNum = date('N',$date);
                $day = getDayFromNum($dayOfWeekNum);
                if ($calendar[$day] == '1') {
                    // add to pairs
                    $arrdepdate = date('Y-m-d', $date);
                    $service = $calendar['serviceId'];
                    $date_serviceIds = addDateServiceId($date_serviceIds, $arrdepdate, $service);
                }
            }
        } else {
            // No intersection with given parameters
        }
    }

    // Parse calendarDates
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
    // There are only calendarDates, so there can be a LOT of serviceIds
    // Check if start- and/or endDate is given as parameter
    // Otherwise set minimum and maximum of calendar_dates
    if (!isset($argv[1]) || !isset($argv[2])) {
        $sql = "
            SELECT MIN(date) startDate, MAX(date) endDate
              FROM calendarDates
        ";
        $stmt = $entityManager->getConnection()->prepare($sql);
        $stmt->execute();
        $startAndEndDate = $stmt->fetchAll();

        $startDate_ = $startAndEndDate[0]['startDate'];
        $endDate_ = $startAndEndDate[0]['endDate'];

        if (isset($argv[1])) {
            $startDate_ = $argv[1];
        }
        if (isset($arg[2])) {
            $endDate_ = $argv[2];
        }
    } else {
        $startDate_ = $argv[1];
        $endDate_ = $argv[2];
    }

    // loop all days between start_date and end_date
    for ($date = strtotime($startDate_); $date < strtotime($endDate_); $date = strtotime('+1 day', $date)) {
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

/**
 * Returns day of week in string representation
 *
 * @param int $dayOfWeekNum Number that represents day of week, e.g. 1 for monday
 * @return string Day of week
 */
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

/**
 * Adds serviceId to corresponding date in date_serviceIdsArray
 *
 * @param array $date_serviceIdsArray Array of dates with serviceIds on corresponding date.
 * @param string $date Date in YYYY-MM-DD format.
 * @param int $serviceId Service identifier.
 * @return array New date_serviceIdsArray with serviceId added to corresponding date.
 */
function addDateServiceId($date_serviceIdsArray, $date, $serviceId) {
    if (!isset($date_serviceIdsArray[$date])) {
        $date_serviceIdsArray[$date] = [];
    }

    $date_serviceIdsArray[$date][] = $serviceId;
    return $date_serviceIdsArray;
}

/**
 * Removes serviceId of corresponding date in date_serviceIdsArray
 *
 * @param array $date_serviceIdsArray Array of dates with serviceIds on corresponding date.
 * @param string $date Date in YYYY-MM-DD format.
 * @param int $serviceId Service identifier.
 * @return array New date_serviceIdsArray with serviceId removed from corresponding date.
 */
function removeDateServiceId($date_serviceIdsArray, $date, $serviceId) {
    $date_serviceIdsWithoutException = [];

    foreach ($date_serviceIdsArray as $dateId => $serviceIds) {
        if ($date === $dateId) {
            // Now check serviceIds
            for ($k = 0; $k < count($serviceIds); $k++) {
                if ($serviceId === $serviceIds[$k]) {
                    // Do nothing, exception found
                } else {
                    $date_serviceIdsWithoutException[$dateId][] = $serviceIds[$k];
                }
            }
        } else {
            $date_serviceIdsWithoutException[$dateId] = $date_serviceIdsArray[$dateId];
        }
    }

    return $date_serviceIdsWithoutException;
}

/**
 * Adds array of calendarDates to data_serviceIdsArray.
 *
 * @param array $data_serviceIdsArray Array of dates with serviceIds on corresponding date.
 * @param array $calendarDates Array of calendarDates.
 * @return array Data_serviceIdsArray with calendarDates added.
 */
function addCalendarDates($data_serviceIdsArray, $calendarDates) {
    for ($i = 0; $i < count($calendarDates); $i++) {
        $calendarDate = $calendarDates[$i];

        // When exceptionType equals 1 -> add to date_serviceIdsArray
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
 * Loops through date_serviceIdsArray and generates ordered list of arrivals and departures of trips per day.
 * The arrivals and departures are written away to corresponding JSON file each time.
 *
 * @param array $date_serviceIdsArray Array of dates with serviceIds on corresponding date.
 * @param mixed $entityManager Entity manager of Doctrine.
 */
function generateArrivalsDepartures($date_serviceIdsArray, $entityManager)
{
    global $arrivalsFilename, $departuresFilename;

    // Loop through all dates
    foreach ($date_serviceIdsArray as $date => $serviceIds) {

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
                '@type' => 'Arrival',
                'date' => $date,
                'gtfs:arrivalTime' => substr($arrivalData['arrivalTime'], 0, 5), // we only need hh:mm
                'gtfs:stop' => $arrivalData['stopId'],
                'gtfs:trip' => $arrivalData['tripId'],
                'gtfs:route' => findRouteId($arrivalData['tripId'], $tripRouteIdPair),
                'gtfs:stopSequence' => $arrivalData['stopSequence'],
                'gtfs:maxStopSequence' => $arrivalData['maxStopSequence']
            ];

            writeToFile($arrivalsFilename, $arrival);
        }

        unset($arrivalsArray); // free memory

        // DEPARTURES
        $departuresArray = queryDepartures($entityManager, $tripMatches);

        for ($z = 0; $z < count($departuresArray); $z++) {
            $departureData = $departuresArray[$z];

            $departure = [
                '@type' => 'Departure',
                'date' => $date,
                'gtfs:departureTime' => substr($departureData['departureTime'], 0, 5), // we only need hh:mm
                'gtfs:stop' => $departureData['stopId'],
                'gtfs:trip' => $departureData['tripId'],
                'gtfs:route' => findRouteId($departureData['tripId'], $tripRouteIdPair),
                'gtfs:stopSequence' => $departureData['stopSequence'],
                'gtfs:maxStopSequence' => $departureData['maxStopSequence']
            ];

            writeToFile($departuresFilename, $departure);
        }
    }

    unset($departuresArray); // free memory
}

/**
 * Queries stoptimes with certain tripIds.
 * These stoptimes are ordered by their arrival time and represent arrival objects.
 *
 * @param mixed $entityManager Entity manager of Doctrine.
 * @param string $trips String of concatenated tripIds (of one day).
 * @return array Stoptimes that are ordered ascending by their arrival time.
 */
function queryArrivals($entityManager, $trips) {
    $sql = "
            SELECT *
              FROM stoptimes
                -- JOIN trips
                --  ON trips.tripId = stoptimes.tripId
                -- JOIN stops
                --  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $trips )
              ORDER BY stoptimes.arrivalTime ASC
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

/**
 * Queries stoptimes with certain tripIds.
 * These stoptimes are ordered by their departure time so we have departure-objects.
 *
 * @param mixed $entityManager Entity manager of Doctrine.
 * @param string $trips String of concatenated tripIds (of one day).
 * @return array Stoptimes that are ordered ascending by their departure time.
 */
function queryDepartures($entityManager, $trips) {
    $sql = "
            SELECT *
              FROM stoptimes
                -- JOIN trips
                --  ON trips.tripId = stoptimes.tripId
                -- JOIN stops
                --  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $trips )
              ORDER BY stoptimes.departureTime ASC
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

/**
 * Search routeId of corresponding tripId.
 *
 * @param int $tripId Holds trip identifier.
 * @param array $tripRouteIdPairs Holds mapping between tripId and routeId.
 * @return int Returns corresponding routeId.
 */
function findRouteId($tripId, $tripRouteIdPairs) {
    for ($i = 0; $i < count($tripRouteIdPairs); $i++) {
        if ($tripRouteIdPairs[$i][0] == $tripId) {
            return $tripRouteIdPairs[$i][1];
        }
    }
}

/**
 * Converts data to JSON and writes/appends this to specified filename.
 *
 * @param string $filename Name of file to write to.
 * @param mixed $data Data to write to file.
 */
function writeToFile($filename, $data) {
    $builder = new JsonBuilder();
    $builder->setValues($data);
    $json = $builder->build();

    file_put_contents($filename, $json.PHP_EOL, FILE_APPEND);
}