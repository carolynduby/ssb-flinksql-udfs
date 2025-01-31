package com.cloudera.ssb.udfs;

import org.apache.flink.table.functions.ScalarFunction;
import ch.hsr.geohash.GeoHash;

/**
 * User defined function for FlinkSQL that takes a latitude and longitude and encodes it as
 * a geohash.
 */
public class LatLonToGeohash extends ScalarFunction {

    private static final int DEFAULT_PRECISION = 12;
    public static final String NULL_GEOHASH = "null_geohash";

    public String eval(Double lat, Double lon) {
        if (lat == null || lon == null) {
            return NULL_GEOHASH;
        } else {
            GeoHash hash = GeoHash.withCharacterPrecision(lat, lon, DEFAULT_PRECISION);
            return hash.toBase32();
        }
    }
}
