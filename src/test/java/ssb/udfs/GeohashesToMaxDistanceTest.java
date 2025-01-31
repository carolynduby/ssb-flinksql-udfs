package ssb.udfs;

import com.cloudera.ssb.udfs.GeohashesToMaxDistance;
import com.cloudera.ssb.udfs.LatLonToGeohash;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.distance.DistanceUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeohashesToMaxDistanceTest {

    private static final LatLonToGeohash LAT_LON_TO_GEOHASH = new LatLonToGeohash();
    private static final GeohashesToMaxDistance GEOHASHES_TO_MAX_DIST = new GeohashesToMaxDistance();
    private static final String nearby1 = LAT_LON_TO_GEOHASH.eval(41.646218, -71.153266);
    private static final String nearby2 = LAT_LON_TO_GEOHASH.eval(41.6271731, -71.1494355);
    private static final String far1 = LAT_LON_TO_GEOHASH.eval(47.6088285, -122.5046034);
    private static final String far2 = LAT_LON_TO_GEOHASH.eval(42.3145014, -71.1348309);
    private static final String far3 = LAT_LON_TO_GEOHASH.eval(48.8588254, 2.2644634);
    private static final String far4 = LAT_LON_TO_GEOHASH.eval(-33.8478035, 150.6016486);

    @Test
    public void testMultiplePoints() {
        assertEquals(2.142, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, nearby1, nearby2)), .001);
        assertEquals(4009.383, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, far2, far1)), .001);
        assertEquals(4042.928, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, nearby1, far1, nearby2, far2)), .001);
        assertEquals(4042.928, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, nearby1, far1, nearby2, far2)), .001);
        assertEquals(16923.379, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, nearby1, far1, far3, nearby2, far2, far4)), .001);
    }

    private double distanceBetweenKm(double lat1, double lon1, double lat2, double lon2) {
        return DistanceUtils.EARTH_MEAN_RADIUS_KM * DistanceUtils.distHaversineRAD(Math.toRadians(lat1), Math.toRadians(lon1), Math.toRadians(lat2), Math.toRadians(lon2));
    }

    @Test
    public void testSinglePoint() {
        assertEquals(0.0F, GEOHASHES_TO_MAX_DIST.eval(String.join(GeohashesToMaxDistance.DELIMITER, nearby1)));
    }

    @Test
    public void testEmptyPoints() {
        assertEquals(0.0F, GEOHASHES_TO_MAX_DIST.eval(""));
    }

    @Test
    public void testNullPoints() {
        assertEquals(0.0F, GEOHASHES_TO_MAX_DIST.eval(null));
    }
}
