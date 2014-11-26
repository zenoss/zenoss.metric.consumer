package org.zenoss.app.consumer.metric.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class BinaryDecoder {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Map<Integer, String> dictionary = new HashMap<>();

    private static class EncodedMetric {
        final long timestamp;
        final int metric;
        final double value;
        final Map<Integer, Integer> tags;

        public EncodedMetric(long timestamp, int metric, double value, Map<Integer, Integer> tags) {
            this.timestamp = timestamp;
            this.metric = metric;
            this.value = value;
            this.tags = tags;
        }
    }

    private String translate(int encoded) {
        return dictionary.get(encoded);
    }

    public Message decode(byte[] data) throws IOException {
        Message msg = new Message();
        DataInputStream stream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(data)));
        byte apiVersion = stream.readByte();
        short numMetrics = stream.readShort();
        EncodedMetric[] encodedMetrics = new EncodedMetric[numMetrics];
        Metric[] metrics = new Metric[numMetrics];
        msg.setMetrics(metrics);
        for (int i = 0; i < numMetrics; i++) {
            long timestamp = (long) stream.readDouble();
            int metricEnc = stream.readInt();
            double metricVal = stream.readDouble();
            Map<Integer, Integer> encodedTags = new HashMap<>();
            byte numTags = stream.readByte();
            for (int j = 0; j < numTags; j++) {
                encodedTags.put(stream.readInt(), stream.readInt());
            }
            encodedMetrics[i] = new EncodedMetric(timestamp, metricEnc, metricVal, encodedTags);
        }
        String json = CharStreams.toString(new InputStreamReader(stream, "UTF-8"));
        Map<String, String> map = mapper.readValue(json, new TypeReference<HashMap<String, String>>() {
        });
        for (Map.Entry<String, String> entry : map.entrySet()) {
            dictionary.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        for (int z = 0; z < encodedMetrics.length; z++) {
            EncodedMetric em = encodedMetrics[z];
            Metric met = new Metric(translate(em.metric), em.timestamp, em.value);
            for (Map.Entry<Integer, Integer> entry : em.tags.entrySet()) {
                met.addTag(translate(entry.getKey()), translate(entry.getValue()));
            }
            metrics[z] = met;
        }
        msg.setMetrics(metrics);
        return msg;
    }
}
