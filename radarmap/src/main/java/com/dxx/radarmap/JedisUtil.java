package com.dxx.radarmap;

import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;

public class JedisUtil {
    public static Jedis getJedis() {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.select(4);
        return jedis;
    }

    public static TileContext getTileContext() throws IOException {
        File file = new File("/Users/xxdeng/Desktop/radar.tif");
        TileContext tileContext = new TileContext(file);
        return tileContext;
    }
}
