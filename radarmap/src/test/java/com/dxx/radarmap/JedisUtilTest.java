package com.dxx.radarmap;

import org.apache.avro.TestAnnotation;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.FileOutputStream;
import java.io.IOException;

public class JedisUtilTest {
    @Test
    public void test1() throws IOException {
        Jedis jedis = JedisUtil.getJedis();
        String hkey= "radarmap";
        String subKey = "5/8/12";
        byte[] bytes = jedis.hget(hkey.getBytes(), subKey.getBytes());


        FileOutputStream outputStream = new FileOutputStream("/Users/xxdeng/Desktop/tmp/5-8-12.png");
        outputStream.write(bytes);
    }


}
