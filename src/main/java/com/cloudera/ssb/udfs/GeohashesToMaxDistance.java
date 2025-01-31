package com.cloudera.ssb.udfs;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.spatial4j.distance.DistanceUtils;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * User Defined Function for FlinkSQL that converts a string containing a comma separated list of locations
 * encoded in Geohashes and calculates the maximum distance in kilometers between every combination of locations.
 */
public class GeohashesToMaxDistance extends ScalarFunction {

    public static final String DELIMITER = ",";

    public Double eval(String hashesCsv) {

        if (hashesCsv != null && hashesCsv.contains(DELIMITER)) {
            List<WGS84Point> locations = Arrays.stream(hashesCsv.split(",")).
                    filter(gh -> !LatLonToGeohash.NULL_GEOHASH.equals(gh)).
                    map(gh -> GeoHash.fromGeohashString(gh).getOriginatingPoint()).collect(Collectors.toList());

            return maxDistancePoints(locations);
        } else {
            return 0.0;
        }
    }

    private double maxDistancePoints(List<WGS84Point> locations) {

        int i = 0;
        int numLocations = locations.size();
        double maxDistance = 0.0;
        for (WGS84Point location1 : locations) {
            int j = numLocations - 1;

            // compare unique combination of points
            ListIterator<WGS84Point> reverseIter = locations.listIterator(numLocations);
            while (reverseIter.hasPrevious()) {
                if (j > i) {
                    WGS84Point location2 = reverseIter.previous();
                    maxDistance = Math.max(maxDistance, DistanceUtils.EARTH_MEAN_RADIUS_KM * DistanceUtils.distHaversineRAD(Math.toRadians(location1.getLatitude()), Math.toRadians(location1.getLongitude())
                            , Math.toRadians(location2.getLatitude()), Math.toRadians(location2.getLongitude())));
                    j--;
                } else {
                    break;
                }
            }
            i++;
        }
        return maxDistance;
    }

}
