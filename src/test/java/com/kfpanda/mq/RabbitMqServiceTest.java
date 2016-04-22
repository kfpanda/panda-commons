package com.kfpanda.mq;

import com.kfpanda.mq.svr.RabbitMqService;
import org.testng.annotations.Test;

/**
 * Created by kfpanda on 16-4-21.
 */
public class RabbitMqServiceTest {

    private RabbitMqService rabbitMqService = new RabbitMqService();
    @Test
    public void publc() throws Exception {
        rabbitMqService.directPublish("amq.direct", "route.key", "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggg", true);
        long start = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++) {
            rabbitMqService.directPublish("amq.direct", "route.key", "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggg", true);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
