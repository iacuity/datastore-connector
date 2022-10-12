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

var (
	_geolocation geolocation
)

type geolocation interface {
	geolocationByIp(ip string) (*Location, error)
}

func InitGeoLocation(location geolocation) {
	_geolocation = location
}

func GeoLocation(ip string) (*Location, error) {
	return _geolocation.geolocationByIp(ip)
}

func IsIPV4(ip string) bool {
	ipV4 := true
	if -1 == strings.Index(ip, ".") {
		ipV4 = false
	}

	return ipV4
}
