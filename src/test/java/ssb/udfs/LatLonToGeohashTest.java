package ssb.udfs;

import com.cloudera.ssb.udfs.LatLonToGeohash;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LatLonToGeohashTest {
    private static final LatLonToGeohash LAT_LON_TO_GEOHASH = new LatLonToGeohash();

    @Test
    public void testLatLonToGeohash() {
        assertEquals("drmkuv7yb9xr", LAT_LON_TO_GEOHASH.eval(41.646218, -71.153266));
    }

    @Test
    public void testNullLatLonGeohash() {
        assertEquals(LatLonToGeohash.NULL_GEOHASH, LAT_LON_TO_GEOHASH.eval(null, -71.153266));
        assertEquals(LatLonToGeohash.NULL_GEOHASH, LAT_LON_TO_GEOHASH.eval(41.646218, null));
        assertEquals(LatLonToGeohash.NULL_GEOHASH, LAT_LON_TO_GEOHASH.eval(null, null));
    }
}
