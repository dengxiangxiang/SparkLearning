package com.dxx.weather;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import util.DbUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

public class RedisToJava {
    static int count = 0;

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        try {
            getAllKeys(jedis, null);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void getAllKeys(Jedis jedis, Consumer<String> consumer) throws SQLException {

        String cursor = ScanParams.SCAN_POINTER_START;

        String key = "*";

        ScanParams scanParams = new ScanParams();

        scanParams.match(key);// 匹配以* 为前缀的 key

        scanParams.count(100);

        while (true) {
// 使用scan命令获取100条数据，使用cursor游标记录位置，下次循环使用

            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);

            cursor = scanResult.getStringCursor();// 返回0 说明遍历完成

            List<String> list = scanResult.getResult();


            for (String string : list) {
                count += list.size();
                System.out.println(count);
                Connection connection = DbUtil.getConnection();
                insertMysql(list, connection);

            }

            if ("0".equals(cursor)) {
                break;
            }

        }

        System.out.println("ok");

    }

    private static Jedis getJedis() {
        String host = "ec2t-weatherredis-01.tst.mypna.com";
        int port = 6379;
        int dbNumber = 3;
        Jedis jedis = new Jedis(host, port);
        jedis.select(dbNumber);
        return jedis;
    }

    private static void insertMysql(List<String> keys, Connection connection) throws SQLException {
        if (keys.size() == 0) {
            return;
        }

        String sql = "insert into sxm_hourlyforecast_redis";
        sql += " values ";

        for (String key : keys) {
            sql += String.format("('%s'),", key);
        }

        int length = sql.length();
        sql = sql.substring(0, length - 1);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();


    }


    public class InternalConsumer implements Consumer<String> {

        private HashSet<String> hashSet = new HashSet<>();
        private int totalCount = 0;

        @Override
        public void accept(String s) {

        }
    }


}

