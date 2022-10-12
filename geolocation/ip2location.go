package geolocation

import (
	"github.com/ip2location/ip2location-go/v9"
)

type ip2Location struct {
	ipV4DB *ip2location.DB
	ipV6DB *ip2location.DB
}

type IP2LocationConfig struct {
	IPV4DBBINFile string
	IPV6DBBINFile string
}

func newIP2Location(cfg IP2LocationConfig) (location *ip2Location, err error) {
	location = &ip2Location{}

	location.ipV4DB, err = ip2location.OpenDB(cfg.IPV4DBBINFile)
	if err != nil {
		return
	}

	location.ipV6DB, err = ip2location.OpenDB(cfg.IPV6DBBINFile)
	if err != nil {
		return
	}

	return &ip2Location{}, nil
}

func (location *ip2Location) geolocationByIp(ip string) (loc *Location, err error) {
	var results ip2location.IP2Locationrecord
	if IsIPV4(ip) {
		results, err = location.ipV4DB.Get_all(ip)
	} else {
		results, err = location.ipV6DB.Get_all(ip)
	}

	if nil != err {
		return
	}

	loc = &Location{
		CountryName: results.Country_long,
		CountryISO2: results.Country_short,
		RegionName:  results.Region,
		City:        results.City,
		ZipCode:     results.Zipcode,
		Latitude:    (float64)(results.Latitude),
		Longitude:   (float64)(results.Longitude),
	}

	return
}

func (location *ip2Location) close() {
	if nil != location {
		if nil != location.ipV4DB {
			location.ipV4DB.Close()
		}
		if nil != location.ipV6DB {
			location.ipV6DB.Close()
		}
	}
}
