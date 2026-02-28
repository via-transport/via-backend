// Package geo provides geographic calculations.
package geo

import "math"

// HaversineDistance returns the great-circle distance in metres between two
// WGS-84 coordinates.
func HaversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6_371_000 // Earth radius in metres
	p1 := lat1 * math.Pi / 180
	p2 := lat2 * math.Pi / 180
	dp := (lat2 - lat1) * math.Pi / 180
	dl := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dp/2)*math.Sin(dp/2) +
		math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}
