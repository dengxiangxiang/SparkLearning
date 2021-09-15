package com.dxx.radarmap;

public class MathUtil {
    static double  math_sinh(double x)
    {
        return (Math.exp(x) - Math.exp(-x)) / 2;
    }

    static double getMapSize(int level)
    {
        return Math.pow(2, level);
    }

    static double lonToTileX(double longitude, int level)
    {
        double x = (longitude + 180) / 360;
        double tileX = Math.floor(x * getMapSize(level));
        return tileX;
    }

    static double latToTileY(double latitude, int level)
    {
        double lat_rad = latitude * Math.PI / 180;
        double y = (1 - Math.log(Math.tan(lat_rad) + 1 / Math.cos(lat_rad)) / Math.PI) / 2;
        double tileY = Math.floor(y * getMapSize(level));
        return tileY;
    }

    static double tileXToLon(int tileX, int level)
    {
        double longitude = tileX / getMapSize(level) * 360 - 180;
        return longitude;
    }

    static double tileYToLat(int tileY, int level)
    {
        double latitude = Math.atan(math_sinh(Math.PI * (1 - 2 * tileY / getMapSize(level)))) * 180.0 / Math.PI;
        return latitude;
    }

    public static void main(String[] args) {
        double lon0 = MathUtil.tileXToLon(4, 4);
        double lon1 = MathUtil.tileXToLon(5, 4);
        System.out.println(lon0);
        System.out.println(lon1);

        double lat0 = MathUtil.tileYToLat(6, 4);
        double lat1 = MathUtil.tileYToLat(7, 4);
        System.out.println(lat0);
        System.out.println(lat1);

        double tileY = MathUtil.latToTileY(30, 7);
        double tileX = MathUtil.lonToTileX(-100, 7);

        System.out.println();
    }
}
