// Commands from https://redis.io/commands#geo

package miniredis

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsGeo handles GEOADD, GEORADIUS etc.
func commandsGeo(m *Miniredis) {
	m.srv.Register("GEOADD", m.cmdGeoadd)
	m.srv.Register("GEODIST", m.cmdGeodist)
	m.srv.Register("GEOPOS", m.cmdGeopos)
	m.srv.Register("GEORADIUS", m.cmdGeoradius)
	m.srv.Register("GEORADIUS_RO", m.cmdGeoradius)
	m.srv.Register("GEORADIUSBYMEMBER", m.cmdGeoradiusbymember)
	m.srv.Register("GEORADIUSBYMEMBER_RO", m.cmdGeoradiusbymember)
	m.srv.Register("GEOSEARCH", m.cmdGeosearch)
}

// GEOADD
func (m *Miniredis) cmdGeoadd(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(3)) {
		return
	}

	if len(args[1:])%3 != 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	key, args := args[0], args[1:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != keyTypeSortedSet {
			c.WriteError(ErrWrongType.Error())
			return
		}

		toSet := map[string]float64{}
		for len(args) > 2 {
			rawLong, rawLat, name := args[0], args[1], args[2]
			args = args[3:]
			longitude, err := strconv.ParseFloat(rawLong, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}
			latitude, err := strconv.ParseFloat(rawLat, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}

			if latitude < -85.05112878 ||
				latitude > 85.05112878 ||
				longitude < -180 ||
				longitude > 180 {
				c.WriteError(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", longitude, latitude))
				return
			}

			toSet[name] = float64(toGeohash(longitude, latitude))
		}

		set := 0
		for name, score := range toSet {
			if db.ssetAdd(key, score, name) {
				set++
			}
		}
		c.WriteInt(set)
	})
}

// GEODIST
func (m *Miniredis) cmdGeodist(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(3)) {
		return
	}

	key, from, to, args := args[0], args[1], args[2], args[3:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		if !db.exists(key) {
			c.WriteNull()
			return
		}
		if db.t(key) != keyTypeSortedSet {
			c.WriteError(ErrWrongType.Error())
			return
		}

		unit := "m"
		if len(args) > 0 {
			unit, args = args[0], args[1:]
		}
		if len(args) > 0 {
			c.WriteError(msgSyntaxError)
			return
		}

		toMeter := parseUnit(unit)
		if toMeter == 0 {
			c.WriteError(msgUnsupportedUnit)
			return
		}

		members := db.sortedsetKeys[key]
		fromD, okFrom := members.get(from)
		toD, okTo := members.get(to)
		if !okFrom || !okTo {
			c.WriteNull()
			return
		}

		fromLo, fromLat := fromGeohash(uint64(fromD))
		toLo, toLat := fromGeohash(uint64(toD))

		dist := distance(fromLat, fromLo, toLat, toLo) / toMeter
		c.WriteBulk(fmt.Sprintf("%.4f", dist))
	})
}

// GEOPOS
func (m *Miniredis) cmdGeopos(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(1)) {
		return
	}

	key, args := args[0], args[1:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != keyTypeSortedSet {
			c.WriteError(ErrWrongType.Error())
			return
		}

		c.WriteLen(len(args))
		for _, l := range args {
			if !db.ssetExists(key, l) {
				c.WriteLen(-1)
				continue
			}
			score := db.ssetScore(key, l)
			c.WriteLen(2)
			long, lat := fromGeohash(uint64(score))
			c.WriteBulk(fmt.Sprintf("%f", long))
			c.WriteBulk(fmt.Sprintf("%f", lat))
		}
	})
}

type geoDistance struct {
	Name      string
	Score     float64
	Distance  float64
	Longitude float64
	Latitude  float64
}

// GEORADIUS and GEORADIUS_RO
func (m *Miniredis) cmdGeoradius(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(5)) {
		return
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
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
	toMeter := parseUnit(args[4])
	if toMeter == 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	args = args[5:]

	var opts struct {
		withDist      bool
		withCoord     bool
		direction     direction // unsorted
		count         int
		withStore     bool
		storeKey      string
		withStoredist bool
		storedistKey  string
	}
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch strings.ToUpper(arg) {
		case "WITHCOORD":
			opts.withCoord = true
		case "WITHDIST":
			opts.withDist = true
		case "ASC":
			opts.direction = asc
		case "DESC":
			opts.direction = desc
		case "COUNT":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			n, err := strconv.Atoi(args[0])
			if err != nil {
				setDirty(c)
				c.WriteError(msgInvalidInt)
				return
			}
			if n <= 0 {
				setDirty(c)
				c.WriteError("ERR COUNT must be > 0")
				return
			}
			args = args[1:]
			opts.count = n
		case "STORE":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			opts.withStore = true
			opts.storeKey = args[0]
			args = args[1:]
		case "STOREDIST":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			opts.withStoredist = true
			opts.storedistKey = args[0]
			args = args[1:]
		default:
			setDirty(c)
			c.WriteError("ERR syntax error")
			return
		}
	}

	if strings.ToUpper(cmd) == "GEORADIUS_RO" && (opts.withStore || opts.withStoredist) {
		setDirty(c)
		c.WriteError("ERR syntax error")
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		if (opts.withStore || opts.withStoredist) && (opts.withDist || opts.withCoord) {
			c.WriteError("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options")
			return
		}

		db := m.db(ctx.selectedDB)
		members := db.ssetElements(key)

		matches := withinRadius(members, longitude, latitude, radius*toMeter)

		// deal with ASC/DESC
		if opts.direction != unsorted {
			sort.Slice(matches, func(i, j int) bool {
				if opts.direction == desc {
					return matches[i].Distance > matches[j].Distance
				}
				return matches[i].Distance < matches[j].Distance
			})
		}

		// deal with COUNT
		if opts.count > 0 && len(matches) > opts.count {
			matches = matches[:opts.count]
		}

		// deal with "STORE x"
		if opts.withStore {
			db.del(opts.storeKey, true)
			for _, member := range matches {
				db.ssetAdd(opts.storeKey, member.Score, member.Name)
			}
			c.WriteInt(len(matches))
			return
		}

		// deal with "STOREDIST x"
		if opts.withStoredist {
			db.del(opts.storedistKey, true)
			for _, member := range matches {
				db.ssetAdd(opts.storedistKey, member.Distance/toMeter, member.Name)
			}
			c.WriteInt(len(matches))
			return
		}

		c.WriteLen(len(matches))
		for _, member := range matches {
			if !opts.withDist && !opts.withCoord {
				c.WriteBulk(member.Name)
				continue
			}

			len := 1
			if opts.withDist {
				len++
			}
			if opts.withCoord {
				len++
			}
			c.WriteLen(len)
			c.WriteBulk(member.Name)
			if opts.withDist {
				c.WriteBulk(fmt.Sprintf("%.4f", member.Distance/toMeter))
			}
			if opts.withCoord {
				c.WriteLen(2)
				c.WriteBulk(fmt.Sprintf("%f", member.Longitude))
				c.WriteBulk(fmt.Sprintf("%f", member.Latitude))
			}
		}
	})
}

// GEORADIUSBYMEMBER and GEORADIUSBYMEMBER_RO
func (m *Miniredis) cmdGeoradiusbymember(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(4)) {
		return
	}

	opts := struct {
		key     string
		member  string
		radius  float64
		toMeter float64

		withDist      bool
		withCoord     bool
		direction     direction // unsorted
		count         int
		withStore     bool
		storeKey      string
		withStoredist bool
		storedistKey  string
	}{
		key:    args[0],
		member: args[1],
	}

	r, err := strconv.ParseFloat(args[2], 64)
	if err != nil || r < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	opts.radius = r

	opts.toMeter = parseUnit(args[3])
	if opts.toMeter == 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	args = args[4:]

	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch strings.ToUpper(arg) {
		case "WITHCOORD":
			opts.withCoord = true
		case "WITHDIST":
			opts.withDist = true
		case "ASC":
			opts.direction = asc
		case "DESC":
			opts.direction = desc
		case "COUNT":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			n, err := strconv.Atoi(args[0])
			if err != nil {
				setDirty(c)
				c.WriteError(msgInvalidInt)
				return
			}
			if n <= 0 {
				setDirty(c)
				c.WriteError("ERR COUNT must be > 0")
				return
			}
			args = args[1:]
			opts.count = n
		case "STORE":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			opts.withStore = true
			opts.storeKey = args[0]
			args = args[1:]
		case "STOREDIST":
			if len(args) == 0 {
				setDirty(c)
				c.WriteError("ERR syntax error")
				return
			}
			opts.withStoredist = true
			opts.storedistKey = args[0]
			args = args[1:]
		default:
			setDirty(c)
			c.WriteError("ERR syntax error")
			return
		}
	}

	if strings.ToUpper(cmd) == "GEORADIUSBYMEMBER_RO" && (opts.withStore || opts.withStoredist) {
		setDirty(c)
		c.WriteError("ERR syntax error")
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		if (opts.withStore || opts.withStoredist) && (opts.withDist || opts.withCoord) {
			c.WriteError("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options")
			return
		}

		db := m.db(ctx.selectedDB)
		if !db.exists(opts.key) {
			c.WriteNull()
			return
		}

		if db.t(opts.key) != keyTypeSortedSet {
			c.WriteError(ErrWrongType.Error())
			return
		}

		// get position of member
		if !db.ssetExists(opts.key, opts.member) {
			c.WriteError("ERR could not decode requested zset member")
			return
		}
		score := db.ssetScore(opts.key, opts.member)
		longitude, latitude := fromGeohash(uint64(score))

		members := db.ssetElements(opts.key)
		matches := withinRadius(members, longitude, latitude, opts.radius*opts.toMeter)

		// deal with ASC/DESC
		if opts.direction != unsorted {
			sort.Slice(matches, func(i, j int) bool {
				if opts.direction == desc {
					return matches[i].Distance > matches[j].Distance
				}
				return matches[i].Distance < matches[j].Distance
			})
		}

		// deal with COUNT
		if opts.count > 0 && len(matches) > opts.count {
			matches = matches[:opts.count]
		}

		// deal with "STORE x"
		if opts.withStore {
			db.del(opts.storeKey, true)
			for _, member := range matches {
				db.ssetAdd(opts.storeKey, member.Score, member.Name)
			}
			c.WriteInt(len(matches))
			return
		}

		// deal with "STOREDIST x"
		if opts.withStoredist {
			db.del(opts.storedistKey, true)
			for _, member := range matches {
				db.ssetAdd(opts.storedistKey, member.Distance/opts.toMeter, member.Name)
			}
			c.WriteInt(len(matches))
			return
		}

		c.WriteLen(len(matches))
		for _, member := range matches {
			if !opts.withDist && !opts.withCoord {
				c.WriteBulk(member.Name)
				continue
			}

			len := 1
			if opts.withDist {
				len++
			}
			if opts.withCoord {
				len++
			}
			c.WriteLen(len)
			c.WriteBulk(member.Name)
			if opts.withDist {
				c.WriteBulk(fmt.Sprintf("%.4f", member.Distance/opts.toMeter))
			}
			if opts.withCoord {
				c.WriteLen(2)
				c.WriteBulk(fmt.Sprintf("%f", member.Longitude))
				c.WriteBulk(fmt.Sprintf("%f", member.Latitude))
			}
		}
	})
}

func withinRadius(members []ssElem, longitude, latitude, radius float64) []geoDistance {
	matches := []geoDistance{}
	for _, el := range members {
		elLo, elLat := fromGeohash(uint64(el.score))
		distanceInMeter := distance(latitude, longitude, elLat, elLo)

		if distanceInMeter <= radius {
			matches = append(matches, geoDistance{
				Name:      el.member,
				Score:     el.score,
				Distance:  distanceInMeter,
				Longitude: elLo,
				Latitude:  elLat,
			})
		}
	}
	return matches
}

func parseUnit(u string) float64 {
	switch strings.ToLower(u) {
	case "m":
		return 1
	case "km":
		return 1000
	case "mi":
		return 1609.34
	case "ft":
		return 0.3048
	default:
		return 0
	}
}

type tuple struct {
	a float64
	b float64
}

type geosearchOpts struct {
	key            string
	withFromMember bool
	fromMember     string
	withFromLonLat bool
	fromLonLat     tuple
	withByRadius   bool
	byRadius       float64
	withByBox      bool
	byBox          tuple
	direction      direction // unsorted
	count          int
	withAny        bool
	withCoord      bool
	withDist       bool
	withHash       bool
}

func geosearchParse(cmd string, args []string) (*geosearchOpts, error) {
	var opts geosearchOpts

	opts.key, args = args[0], args[1:]

	fmt.Printf("args: %v\n", args)

	switch strings.ToUpper(args[0]) {
	case "FROMMEMBER":
		if len(args) < 2 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.withFromMember = true
		opts.fromMember = args[1]
		args = args[2:]
	case "FROMLONLAT":
		if len(args) < 3 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.withFromLonLat = true
		if err := optFloat(args[1], &opts.fromLonLat.a); err != nil {
			return nil, err
		}
		if err := optFloat(args[2], &opts.fromLonLat.b); err != nil {
			return nil, err
		}
		args = args[3:]
	default:
		return nil, errors.New(errWrongNumber(cmd))
	}

	if len(args) < 3 {
		return nil, errors.New(errWrongNumber(cmd))
	}
	switch strings.ToUpper(args[0]) {
	case "BYRADIUS":
		if len(args) < 3 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.withByRadius = true
		if err := optFloat(args[1], &opts.byRadius); err != nil {
			return nil, err
		}
		toMeter := parseUnit(args[2])
		if toMeter == 0 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.byRadius *= toMeter
		args = args[3:]
	case "BYBOX":
		if len(args) < 4 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.withByBox = true
		if err := optFloat(args[1], &opts.byBox.a); err != nil {
			return nil, err
		}
		if err := optFloat(args[2], &opts.byBox.b); err != nil {
			return nil, err
		}
		toMeter := parseUnit(args[3])
		if toMeter == 0 {
			return nil, errors.New(errWrongNumber(cmd))
		}
		opts.byBox.a *= toMeter
		opts.byBox.b *= toMeter
		args = args[4:]
	default:
		return nil, errors.New(errWrongNumber(cmd))
	}

	// FIXME: ASC|DESC
	// FIXME: COUNT n ANY
	// FIXME: WITHCOORD
	// FIXME: WITHDIST
	// FIXME: WITHHASH

	return &opts, nil
}

// GEOSEARCH
func (m *Miniredis) cmdGeosearch(c *server.Peer, cmd string, args []string) {
	if len(args) < 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c, cmd) {
		return
	}

	opts, err := geosearchParse(cmd, args)
	if err != nil {
		setDirty(c)
		c.WriteError(err.Error())
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		members := db.ssetElements(opts.key)

		if !opts.withFromLonLat {
			panic("wip")
		}
		if !opts.withByRadius {
			panic("wip")
		}
		matches := withinRadius(members, opts.fromLonLat.a, opts.fromLonLat.b, opts.byRadius)

		/*
			// deal with ASC/DESC
			if opts.direction != unsorted {
				sort.Slice(matches, func(i, j int) bool {
					if opts.direction == desc {
						return matches[i].Distance > matches[j].Distance
					}
					return matches[i].Distance < matches[j].Distance
				})
			}

			// deal with COUNT
			if opts.count > 0 && len(matches) > opts.count {
				matches = matches[:opts.count]
			}
		*/

		c.WriteLen(len(matches))
		for _, member := range matches {
			// if !opts.withDist && !opts.withCoord {
			c.WriteBulk(member.Name)
			continue
			// }

			/*
				len := 1
				if opts.withDist {
					len++
				}
				if opts.withCoord {
					len++
				}
				c.WriteLen(len)
				c.WriteBulk(member.Name)
				if opts.withDist {
					c.WriteBulk(fmt.Sprintf("%.4f", member.Distance/toMeter))
				}
				if opts.withCoord {
					c.WriteLen(2)
					c.WriteBulk(fmt.Sprintf("%f", member.Longitude))
					c.WriteBulk(fmt.Sprintf("%f", member.Latitude))
				}
			*/
		}
	})
}
