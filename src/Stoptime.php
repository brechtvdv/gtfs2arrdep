<?php

/**
 * Class Stoptime
 */
class Stoptime
{
    /**
     * @var int
     */
    protected $id;
    /**
     * @var string
     */
    protected $tripId;
    /**
     * @var \DateTime
     */
    protected $arrivalTime;
    /**
     * @var \DateTime
     */
    protected $departureTime;
    /**
     * @var string
     */
    protected $stopId;
    /**
     * @var int
     */
    protected $stopSequence;
    /**
     * @var string
     */
    protected $stopHeadsign = null;
    /**
     * @var int
     */
    protected $pickupType = null;
    /**
     * @var int
     */
    protected $dropOffType = null;
    /**
     * @var string
     */
    protected $shapeDistTraveled = null;
    /**
     * @var int
     */
    protected $timepoint = null;

    /**
     * @return int
     */
    public function getId()
    {
        return $this->id;
    }
    /**
     * @return string
     */
    public function getTripId()
    {
        return $this->tripId;
    }

    /**
     * @param string $tripId
     */
    public function setTripId($tripId)
    {
        $this->tripId = $tripId;
    }

    /**
     * @return \DateTime
     */
    public function getArrivalTime()
    {
        return $this->arrivalTime;
    }

    /**
     * @param \DateTime $arrivalTime
     */
    public function setArrivalTime($arrivalTime)
    {
        $this->arrivalTime = $arrivalTime;
    }

    /**
     * @return \DateTime
     */
    public function getDepartureTime()
    {
        return $this->departureTime;
    }

    /**
     * @param \DateTime $departureTime
     */
    public function setDepartureTime($departureTime)
    {
        $this->departureTime = $departureTime;
    }

    /**
     * @return string
     */
    public function getStopId()
    {
        return $this->stopId;
    }

    /**
     * @param string $stopId
     */
    public function setStopId($stopId)
    {
        $this->stopId = $stopId;
    }

    /**
     * @return int
     */
    public function getStopSequence()
    {
        return $this->stopSequence;
    }

    /**
     * @param int $stopSequence
     */
    public function setStopSequence($stopSequence)
    {
        $this->stopSequence = $stopSequence;
    }

    /**
     * @return string
     */
    public function getStopHeadsign()
    {
        return $this->stopHeadsign;
    }

    /**
     * @param string $stopHeadsign
     */
    public function setStopHeadsign($stopHeadsign)
    {
        $this->stopHeadsign = $stopHeadsign;
    }

    /**
     * @return int
     */
    public function getPickupType()
    {
        return $this->pickupType;
    }

    /**
     * @param int $pickupType
     */
    public function setPickupType($pickupType)
    {
        $this->pickupType = $pickupType;
    }

    /**
     * @return int
     */
    public function getDropOffType()
    {
        return $this->dropOffType;
    }

    /**
     * @param int $dropOffType
     */
    public function setDropOffType($dropOffType)
    {
        $this->dropOffType = $dropOffType;
    }

    /**
     * @return string
     */
    public function getShapeDistTraveled()
    {
        return $this->shapeDistTraveled;
    }

    /**
     * @param string $shapeDistTraveled
     */
    public function setShapeDistTraveled($shapeDistTraveled)
    {
        $this->shapeDistTraveled = $shapeDistTraveled;
    }

    /**
     * @return int
     */
    public function getTimepoint()
    {
        return $this->timepoint;
    }

    /**
     * @param int $timepoint
     */
    public function setTimepoint($timepoint)
    {
        $this->timepoint = $timepoint;
    }
}