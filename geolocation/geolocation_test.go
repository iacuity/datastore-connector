package geolocation

import (
	"testing"
)

func TestIsIPV4(t *testing.T) {
	if !IsIPV4("114.143.159.230") {
		t.Error("Expected to be IPV4")
	}

	if IsIPV4("2a00:1450:4009:822::200e") {
		t.Error("Expected to be IPV6")
	}
}

func TestNewIP2Location(t *testing.T) {
	location, _ := NewIP2Location("", "")
	InitGeoLocation(location)
}
