package miniredis

import (
	"testing"

	"github.com/go-redis/redis"
)

// Test GEOADD / GEORADIUS
func TestGeo(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer client.Close()

	{
		err = client.GeoAdd("Sicily", &redis.GeoLocation{
			Name:      "Palermo",
			Longitude: 13.361389,
			Latitude:  38.115556,
		}).Err()
		ok(t, err)

		err = client.GeoAdd("Sicily", &redis.GeoLocation{
			Name:      "Catania",
			Longitude: 15.087269,
			Latitude:  37.502669,
		}).Err()
		ok(t, err)

		res, err := client.GeoRadius("Sicily", 15, 37, &redis.GeoRadiusQuery{WithDist: true, WithCoord: true, Radius: 200}).Result()
		ok(t, err)
		for _, loc := range res {
			if loc.Name != "Catania" && loc.Name != "Palermo" {
				t.Errorf("unexpected name %q", loc.Name)
			}
			if loc.Latitude == 0.00 || loc.Longitude == 0.00 {
				t.Errorf("latitude/longitude shouldn't be empty")
			}
			if loc.Dist == 0.00 {
				t.Errorf("distance shouldn't be empty")
			}
		}
		equals(t, 2, len(res))
	}
}
