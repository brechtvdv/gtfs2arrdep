<?php

/**
 * Class Agency
 */
class Agency
{
    /**
     * @var int
     */
    protected $agencyId = null;
    /**
     * @var string
     */
    protected $agencyName;
    /**
     * @var string
     */
    protected $agencyUrl;
    /**
     * @var string
     */
    protected $agencyTimezone;
    /**
     * @var string
     */
    protected $agencyLang = null;
    /**
     * @var string
     */
    protected $agencyPhone = null;
    /**
     * @var string
     */
    protected $agencyFareUrl = null;

    public function getAgencyId()
    {
        return $this->agencyId;
    }

    public function setAgencyId($agencyId)
    {
        return $this->agencyId = $agencyId;
    }

    public function getAgencyName()
    {
        return $this->agencyName;
    }

    public function setAgencyName($agencyName)
    {
        $this->agencyName = $agencyName;
    }

    public function getAgencyUrl()
    {
        return $this->agencyUrl;
    }

    public function setAgencyUrl($agencyUrl)
    {
        $this->agencyUrl = $agencyUrl;
    }

    public function getAgencyTimezone()
    {
        return $this->agencyTimezone;
    }

    public function setAgencyTimezone($agencyTimezone)
    {
        $this->agencyTimezone = $agencyTimezone;
    }

    public function getAgencyLang()
    {
        return $this->agencyLang;
    }

    public function setAgencyLang($agencyLang)
    {
        $this->agencyLang = $agencyLang;
    }

    public function getAgencyPhone()
    {
        return $this->agencyPhone;
    }

    public function setAgencyPhone($agencyPhone)
    {
        $this->agencyPhone = $agencyPhone;
    }

    public function getAgencyFareUrl()
    {
        return $this->agencyFareUrl;
    }

    public function setAgencyFareUrl($agencyFareUrl)
    {
        $this->agencyFareUrl = $agencyFareUrl;
    }
}