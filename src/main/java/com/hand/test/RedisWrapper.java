package com.hand.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author jiehui.huang
 * @version 1.0
 * @date 2021/7/26 16:29
 */
public class RedisWrapper {

    private static JedisPool jedisPool;

    public RedisWrapper(JedisPoolConfig config, String host, int port, String pass) {
        config.setMaxTotal(9999);
        config.setMaxIdle(999);
        config.setMinIdle(20);
        jedisPool = new JedisPool(config, host, port, 2000, pass, 0);
    }


    public  synchronized Jedis getJedis() {
        try {
            if (jedisPool != null) {
                return jedisPool.getResource();
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new JedisException(e);
        }
    }

    public  void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
}