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
$agencyId = $agencyArray[0]['agencyId'];
$arrivalsFilename = 'dist/arrivals-' . $agencyId . '.jsonldstream';
$departuresFilename = 'dist/departures-' . $agencyId . '.jsonldstream';

// delete previous files
if (file_exists($arrivalsFilename)) {
    unlink($arrivalsFilename);
}
if (file_exists($departuresFilename)) {
    unlink($departuresFilename);
}

// Write context to the files
$context = createContext($agencyId);
writeToFile($arrivalsFilename, $context);
writeToFile($departuresFilename, $context);

// This holds array of dates with corresponding serviceIds
// We need serviceIds per day to query ordered arrivals/departures
$date_serviceIdsArray = [];

$arrivalNr = 1; // counter for arrival @id
$departureNr = 1; // counter for departure @id

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
            // Arrdeps that happen after midnight of the previous day need to be added too
            $prevDate = strtotime('-1 day', $startDate);
            $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $prevDate);
            $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);

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
                    $date_serviceIdsArray = addDateServiceId($date_serviceIdsArray, $arrdepdate, $service);
                }
            }
        } else if ($startDate < $startDate_ && $endDate <= $endDate_) {
            // Arrdeps that happen after midnight of the previous day need to be added too
            $prevDate = strtotime('-1 day', $startDate_);
            $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $prevDate);
            $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);

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
                    $date_serviceIdsArray = addDateServiceId($date_serviceIdsArray, $arrdepdate, $service);
                }
            }
        } else if ($startDate >= $startDate_ && $endDate > $endDate_) {
            // Arrdeps that happen after midnight of the previous day need to be added too
            $prevDate = strtotime('-1 day', $startDate);
            $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $prevDate);
            $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);

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
                    $date_serviceIdsArray = addDateServiceId($date_serviceIdsArray, $arrdepdate, $service);
                }
            }
        } else if ($startDate < $startDate_ && $endDate > $endDate_) {
            // Arrdeps that happen after midnight of the previous day need to be added too
            $prevDate = strtotime('-1 day', $startDate_);
            $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $prevDate);
            $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);

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
                    $date_serviceIdsArray = addDateServiceId($date_serviceIdsArray, $arrdepdate, $service);
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
    $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);

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

    // Arrdeps that happen after midnight of the previous day need to be added too
    $prevDate = strtotime('-1 day', strtotime($startDate_));
    $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $prevDate);

    $prevDate_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates);
    $date_serviceIdsArray = $prevDate_serviceIdsArray;

    // loop all days between start_date and end_date
    for ($date = strtotime($startDate_); $date <= strtotime($endDate_); $date = strtotime('+1 day', $date)) {
        $calendarDates = getCalendarDatesOfSpecificDate($entityManager, $date);

        $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates); // holds previous date and current date

        generateArrivalsDepartures($date_serviceIdsArray, $entityManager);

        $date_serviceIdsArray = []; // empty again
        $date_serviceIdsArray = addCalendarDates($date_serviceIdsArray, $calendarDates); // Keep track of previous day for the stoptimes after midnight
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
 * @param array $date_serviceIdsArray Array of dates with serviceIds on corresponding date.
 * @param array $calendarDates Array of calendarDates.
 * @return array Date_serviceIdsArray with calendarDates added.
 */
function addCalendarDates($date_serviceIdsArray, $calendarDates) {
    for ($i = 0; $i < count($calendarDates); $i++) {
        $calendarDate = $calendarDates[$i];

        // When exceptionType equals 1 -> add to date_serviceIdsArray
        // When exceptionType equals 2 -> remove
        if ($calendarDate['exceptionType'] == "1") {
            $date_serviceIdsArray = addDateServiceId($date_serviceIdsArray, $calendarDate['date'], $calendarDate['serviceId']);
        } else {
            $date_serviceIdsArray = removeDateServiceId($date_serviceIdsArray, $calendarDate['date'], $calendarDate['serviceId']);
        }
    }
    return $date_serviceIdsArray;
}

/**
 * Adds calendar dates of specific day.
 *
 * @param mixed $entityManager Doctrine entity manager.
 * @param string $date Holds date of day.
 * @return array Date_serviceIdsArray with calendarDates of day added.
 */
function getCalendarDatesOfSpecificDate($entityManager, $date) {
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

    return $calendarDates;
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
    global $arrivalNr, $departureNr;

    $firstLoop = true; // First date is previous date that we need to retrieve stoptimes after midnight

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

        if ($firstLoop) {
            $prevDayTripMatches = $tripMatches;
            $firstLoop = false;
            continue;
        }

        // ARRIVALS
        $arrivalsArray = queryArrivals($entityManager, $tripMatches);

        // Search arrivals that happen after midnight of previous day
        $arrivalsAfterMidnight = queryArrivalsAfterMidnight($entityManager, $prevDayTripMatches);

        // Merge sort
        $i = 0;
        $j = 0;
        while ($i < count($arrivalsArray) && $j < count($arrivalsAfterMidnight)) {
            if ($arrivalsArray[$i]['arrivalTime'] < $arrivalsAfterMidnight[$j]['arrivalTime']) {
                $arrivalData = $arrivalsArray[$i++];
            } else {
                $arrivalData = $arrivalsAfterMidnight[$j++];
            }

            writeToFile($arrivalsFilename, generateArrival($arrivalData, $date, $tripRouteIdPair, $arrivalNr));
            $arrivalNr++;
        }

        while ($i < count($arrivalsArray)) {
            $arrivalData = $arrivalsArray[$i++];
            writeToFile($arrivalsFilename, generateArrival($arrivalData, $date, $tripRouteIdPair, $arrivalNr));
            $arrivalNr++;
        }

        while ($j < count($arrivalsAfterMidnight)) {
            $arrivalData = $arrivalsAfterMidnight[$j++];
            writeToFile($arrivalsFilename, generateArrival($arrivalData, $date, $tripRouteIdPair, $arrivalNr));
            $arrivalNr++;
        }

        unset($arrivalsArray); // free memory

        // DEPARTURES
        $departuresArray = queryDepartures($entityManager, $tripMatches);

        // Search departures that happen after midnight of previous day
        $departuresAfterMidnight = queryDeparturesAfterMidnight($entityManager, $prevDayTripMatches);

        // Merge sort
        $i = 0;
        $j = 0;
        while ($i < count($departuresArray) && $j < count($departuresAfterMidnight)) {
            if ($departuresArray[$i]['departureTime'] < $departuresAfterMidnight[$j]['departureTime']) {
                $departureData = $departuresArray[$i++];
            } else {
                $departureData = $departuresAfterMidnight[$j++];
            }

            writeToFile($departuresFilename, generateDeparture($departureData, $date, $tripRouteIdPair, $departureNr));
            $departureNr++;
        }

        while ($i < count($departuresArray)) {
            $departureData = $departuresArray[$i++];
            writeToFile($departuresFilename, generateDeparture($departureData, $date, $tripRouteIdPair, $departureNr));
            $departureNr++;
        }

        while ($j < count($departuresAfterMidnight)) {
            $departureData = $arrivalsAfterMidnight[$j++];
            writeToFile($departuresFilename, generateDeparture($departureData, $date, $tripRouteIdPair, $departureNr));
            $departureNr++;
        }
        unset($departuresArray); // free memory

        $prevDayTripMatches = $tripMatches;
    }
}

/**
 * @param $arrivalData Stoptime that is sorted by arrivalTime.
 * @param $date Date in HH:MM:SS format.
 * @param $tripRouteIdPair Array that holds mapping between tripIDs and routeIDs.
 * @param int $arrivalNr Number of arrival. Represents a counter.
 * @return array Arrival.
 */
function generateArrival($arrivalData, $date, $tripRouteIdPair, $arrivalNr) {
    return [
        '@type' => 'Arrival',
        '@id' => 'arrival:' . $arrivalNr,
        'date' => $date,
        'arrivalTime' => substr($arrivalData['arrivalTime'], 0, 5), // we only need hh:mm
        'stop' => $arrivalData['stopId'],
        'trip' => $arrivalData['tripId'],
        'route' => findRouteId($arrivalData['tripId'], $tripRouteIdPair),
        'stopSequence' => $arrivalData['stopSequence'],
        'maxStopSequence' => $arrivalData['maxStopSequence']
    ];
}

/**
 * @param $departureData Stoptime that is sorted by departureTime.
 * @param $date Date in HH:MM:SS format.
 * @param $tripRouteIdPair Array that holds mapping between tripIDs and routeIDs.
 * @param int $departureNr Number of departure. Represents a counter.
 * @return array Departure.
 */
function generateDeparture($departureData, $date, $tripRouteIdPair, $departureNr) {
    return [
        '@type' => 'Departure',
        '@id' => 'departure:' . $departureNr,
        'date' => $date,
        'departureTime' => substr($departureData['departureTime'], 0, 5), // we only need hh:mm
        'stop' => $departureData['stopId'],
        'trip' => $departureData['tripId'],
        'route' => findRouteId($departureData['tripId'], $tripRouteIdPair),
        'stopSequence' => $departureData['stopSequence'],
        'maxStopSequence' => $departureData['maxStopSequence']
    ];
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
                AND NOT stoptimes.arrivalAfterMidnight
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
                AND NOT stoptimes.departureAfterMidnight
              ORDER BY stoptimes.departureTime ASC
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

/**
 * Returns arrivals that happen after midnight
 *
 * @param $entityManager Doctrine entity manager.
 * @param $prevDayTripMatches Trips of previous day.
 * @return mixed Arrivals after midnight.
 */
function queryArrivalsAfterMidnight($entityManager, $prevDayTripMatches) {
    $sql = "
            SELECT *
              FROM stoptimes
                -- JOIN trips
                --  ON trips.tripId = stoptimes.tripId
                -- JOIN stops
                --  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $prevDayTripMatches )
                AND stoptimes.arrivalAfterMidnight
              ORDER BY stoptimes.arrivalTime ASC
        ";

    $stmt = $entityManager->getConnection()->prepare($sql);
    $stmt->execute();
    return $stmt->fetchAll();
}

/**
 * Returns departures that happen after midnight
 *
 * @param $entityManager Doctrine entity manager.
 * @param $prevDayTripMatches Trips of previous day.
 * @return mixed Departures after midnight.
 */
function queryDeparturesAfterMidnight($entityManager, $prevDayTripMatches) {
    $sql = "
            SELECT *
              FROM stoptimes
                -- JOIN trips
                --  ON trips.tripId = stoptimes.tripId
                -- JOIN stops
                --  ON stops.stopId = stoptimes.stopId
              WHERE stoptimes.tripId IN ( $prevDayTripMatches )
                AND stoptimes.departureAfterMidnight
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

function createContext($agencyId) {
    return [
        "@context"      =>
        [
            "gtfs"          => "http://vocab.gtfs.org/terms#",
            "trip"          => "gtfs:trip",
            "route"         => "gtfs:route",
            "stopSequence"  => "gtfs:stopSequence",
            "dct"           => "http://purl.org/dc/terms/",
            "date"          => "dct:date",
            "arrival"       => "http://irail.be/arrivals/" . $agencyId . "/",
            "departure"     => "http://irail.be/departures/" . $agencyId . "/",
            "Arrival"       => "http://semweb.mmlab.be/ns/stoptimes#Arrival",
            "arrivalTime"   => "gtfs:arrivalTime",
            "Departure"     => "http://semweb.mmlab.be/ns/stoptimes#Departure",
            "departureTime" => "gtfs:departureTime",
            "stop" => "gtfs:stop"
        ]
    ];
}

/**
 * Converts data to JSON and writes/appends this to specified filename.
 *
 * @param string $filename Name of file to write to.
 * @param mixed $data Data to write to file.
 */
function writeToFile($filename, $data) {
    $builder = new JsonBuilder();
    $builder->setJsonEncodeOptions(JSON_UNESCAPED_SLASHES);
    $builder->setValues($data);
    $json = $builder->build();

    file_put_contents($filename, $json.PHP_EOL, FILE_APPEND);
}