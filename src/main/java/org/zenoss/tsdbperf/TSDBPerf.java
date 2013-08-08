package org.zenoss.tsdbperf;

import com.google.common.collect.Lists;
import org.zenoss.lib.tsdb.OpenTsdbClient;
import org.zenoss.lib.tsdb.OpenTsdbClientConfiguration;
import org.zenoss.lib.tsdb.OpenTsdbClientFactory;
import org.zenoss.lib.tsdb.OpenTsdbClientPoolConfiguration;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TSDBPerf {

        public static void main(String[] args) throws Exception {
                OpenTsdbClientConfiguration clientConfg = new OpenTsdbClientConfiguration();
                clientConfg.setHost("192.168.33.1");
                clientConfg.setPort(4243);
                OpenTsdbClientPoolConfiguration poolConfig = new OpenTsdbClientPoolConfiguration();
                poolConfig.setClientConfiguration(Lists.newArrayList(clientConfg));
                OpenTsdbClientFactory factory = new OpenTsdbClientFactory(poolConfig);
                OpenTsdbClient client = factory.makeObject();
                try {
                        Random rand = new Random();
                        long duration = TimeUnit.SECONDS.toMillis(60);
                        long start = System.currentTimeMillis();
                        long end = start + duration;

                        int idx = 0;
                        HashMap<String, String> tags = new HashMap<String, String>();
                        while (System.currentTimeMillis() < end) {
                                tags.clear();
                                tags.put("tag1", "val1_" + rand.nextInt(5000));
                                tags.put("tag2", "val2_" + rand.nextInt(5000));
                                String put = OpenTsdbClient.toPutMessage("metric_" + rand.nextInt(5000),
                                                                         start + idx, 1000 * rand.nextFloat(), tags);
                                client.put(put);
                                idx++;
                                if (idx % 1000 == 0) {
                                        client.flush();
                                }
                        }
                        client.flush();
                        long elapsed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
                        System.out.println("Proccessed " + idx + " in " + elapsed + " seconds");
                        System.out.println("Proccessed " + idx / elapsed + "/second");

                } finally {
                        client.close();
                }
        }
}
