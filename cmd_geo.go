// Commands from https://redis.io/commands#generic

package miniredis

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2/server"
	"github.com/mmcloughlin/geohash"
)

// commandsGeo handles GEOADD, GEORADIUS etc.
func commandsGeo(m *Miniredis) {
	m.srv.Register("GEOADD", m.cmdGeoAdd)
	m.srv.Register("GEORADIUS", m.cmdGeoRadius)
}

// GEOADD
func (m *Miniredis) cmdGeoAdd(c *server.Peer, cmd string, args []string) {
	if len(args) < 3 || len(args[1:])%3 != 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}
	key := args[0]

	newArgs := []string{key}
	for i := range args[1:] {
		latitude, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			continue
		}
		longitude, err := strconv.ParseFloat(args[i+1], 64)
		if err != nil {
			continue
		}

		name := args[i+2]
		score := geohash.EncodeIntWithPrecision(longitude, latitude, 64)
		newArgs = append(newArgs, fmt.Sprintf("%d", score))
		newArgs = append(newArgs, name)
	}
	m.cmdZadd(c, "ZADD", newArgs)
}

type geoRadiusResponse struct {
	Name      string
	Distance  float64
	Longitude float64
	Latitude  float64
}

func (m *Miniredis) cmdGeoRadius(c *server.Peer, cmd string, args []string) {
	var (
		withDist  = false
		withCoord = false
	)

	if len(args) < 5 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil || longitude < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil || latitude < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	radius, err := strconv.ParseFloat(args[3], 64)
	if err != nil || radius < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	unit := args[4]
	switch unit {
	case "m":
		break
	case "km":
		radius = radius * 1000
	case "mi":
		radius = radius * 1609.34
	case "ft":
		radius = radius * 0.3048
	default:
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	for _, arg := range args[4:] {
		switch strings.ToUpper(arg) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		}
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		members := db.ssetElements(key)

		membersWithinRadius := []geoRadiusResponse{}
		for _, el := range members {
			elLat, elLo := geohash.DecodeIntWithPrecision(uint64(el.score), 64)
			distanceInMeter := distance(latitude, longitude, elLat, elLo)

			if distanceInMeter <= radius {
				membersWithinRadius = append(membersWithinRadius, geoRadiusResponse{
					Name:      el.member,
					Distance:  distanceInMeter,
					Longitude: longitude,
					Latitude:  latitude,
				})
			}
		}

		c.WriteLen(len(membersWithinRadius))
		for _, member := range membersWithinRadius {
			if withDist {
				if withCoord {
					c.WriteLen(3)
				} else {
					c.WriteLen(2)
				}
				c.WriteBulk(member.Name)
				c.WriteBulk(fmt.Sprintf("%f", member.Distance))
			} else {
				if withCoord {
					c.WriteLen(2)
				} else {
					c.WriteLen(1)
				}
				c.WriteBulk(member.Name)
			}

			if withCoord {
				c.WriteLen(2)
				c.WriteBulk(fmt.Sprintf("%f", member.Longitude))
				c.WriteBulk(fmt.Sprintf("%f", member.Latitude))
			}
		}
	})
}

// haversin(θ) function
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is meters
// http://en.wikipedia.org/wiki/Haversine_formula
// Source: https://gist.github.com/cdipaolo/d3f8db3848278b49db68
func distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}
