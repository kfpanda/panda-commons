package redis;

import com.kfpanda.redis.RedisUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by kfpanda on 16-4-14.
 */
public class RedisTest {

    @Test
    public void set(){
        Jedis redis = RedisUtil.getResource();
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            //redis.hset("test", "kfpanda" + i, "ERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the console.ERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the console.ERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the consoleERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the console.ERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the console.ERROR StatusLogger No log4j2 configuration file found. Using default configuration: logging only errors to the console..");
            redis.hset("test1", "kfpanda" + i, "hello world!");
            if(i % 10000 == 0){
                long endTime = System.currentTimeMillis();
                System.out.println("10000 time = " + (endTime - startTime));
                startTime = endTime;
            }
        }

        System.out.println("end time = " + (System.currentTimeMillis() - startTime));
    }
}
