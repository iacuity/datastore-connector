package geolocation

import (
	"strings"
)

type Location struct {
	ContinentCode string  `json:",omitempty"`
	ContinentName string  `json:",omitempty"`
	CountryName   string  `json:",omitempty"`
	CountryISO2   string  `json:",omitempty"`
	CountryISO3   string  `json:",omitempty"`
	RegionName    string  `json:",omitempty"`
	RegionCode    string  `json:",omitempty"`
	City          string  `json:",omitempty"`
	ZipCode       string  `json:",omitempty"`
	Latitude      float64 `json:",omitempty"`
	Longitude     float64 `json:",omitempty"`
}

const (
	GBISO2 = "GB"
	UKISO2 = "UK"
)

var (
	_geolocation geolocation
)

type geolocation interface {
	geolocationByIp(ip string) (*Location, error)
	close()
}

func InitGeoLocation(cfg IP2LocationConfig) (err error) {
	_geolocation, err = newIP2Location(cfg)
	return
}

func GeoLocation(ip string) (*Location, error) {
	loc, err := _geolocation.geolocationByIp(ip)
	if nil == err {
		if GBISO2 == loc.CountryISO2 {
			loc.CountryISO2 = UKISO2
		}
	}

	return loc, err
}

func Close() {
	_geolocation.close()
}

func IsIPV4(ip string) bool {
	ipV4 := true
	if -1 == strings.Index(ip, ".") {
		ipV4 = false
	}

	return ipV4
}
