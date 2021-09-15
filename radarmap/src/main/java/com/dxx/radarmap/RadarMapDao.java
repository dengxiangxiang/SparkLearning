package com.dxx.radarmap;

import java.io.IOException;

public interface RadarMapDao {
     void init(String ss);

    void addRadarMap(String timeString, int z, int x, int y, byte[] bytes) throws IOException;

    byte[] getRadarMap( String timeString, int z, int x, int y);
}
