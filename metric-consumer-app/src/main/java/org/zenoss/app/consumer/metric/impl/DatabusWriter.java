/**
 * 
 */
package org.zenoss.app.consumer.metric.impl;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zenoss.app.consumer.metric.DatabusPublishConfig;
import org.zenoss.app.consumer.metric.MetricServiceConfiguration;
import org.zenoss.app.consumer.metric.TsdbWriter;
import org.zenoss.app.consumer.metric.data.Metric;
import org.zenoss.databus.common.DatabusRecord;
import org.zenoss.databus.common.utils.JsonConvertor;
import org.zenoss.databus.producer.DatabusProducer;
import org.zenoss.databus.producer.DatabusProducerConstants;
import org.zenoss.databus.producerpool.DatabusProducerPool;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 
 *
 */
@Component
public class DatabusWriter  implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(DatabusWriter.class);
	private DatabusProducerPool producerPool;
	private String topic = null;
	private DatabusMetricsQueue databusMetricsQueue;
	private JsonConvertor jsonConverter = null;
	private DatabusPublishConfig databusPublishConfig;
	/**
	 * where to report in when running
	 */
	private final DatabusWriterRegistry writerRegistry;

	/**
	 * Size of batches to send to TSDB socket
	 */
	private final int batchSize;

	/**
	 * Max idle time before suicide
	 */
	private final int maxIdleTime;

	/**
	 * Is this instance currently running?
	 */
	private transient boolean running;

	/**
	 * Has this instance been canceled?
	 */
	private transient boolean canceled;

	/**
	 * Last time this instance did work
	 */
	protected transient long lastWorkTime;

	@Autowired
	DatabusWriter(MetricServiceConfiguration config, DatabusWriterRegistry registry, DatabusProducerPool producerPool,
			DatabusMetricsQueue databusMetricsQueue) {
		this.producerPool = producerPool;
		this.databusMetricsQueue = databusMetricsQueue;
		this.writerRegistry = registry;
		this.databusPublishConfig = config.getDatabusPublishConfig();
		this.topic = databusPublishConfig.getProperty(DatabusProducerConstants.KAFKA_TOPIC_NAME);
		this.batchSize = databusPublishConfig.getBatchSize();
		this.maxIdleTime = databusPublishConfig.getMaxIdleTime();
		this.jsonConverter = new JsonConvertor();
		this.running = false;
		this.canceled = false;
		this.lastWorkTime = 0;
	}

	@Override
	public void run() {
		log.info("Starting writer");
		try {
			writerRegistry.register(this);
			running = true;
			runUntilCanceled();
		} catch (InterruptedException ie) {
			log.info("Exiting due to thread interrupt");
			Thread.currentThread().interrupt();
		} catch (RuntimeException e) {
			log.error("Thread exiting due to unexpected exception", e);
			throw e;
		} finally {
			running = false;
			writerRegistry.unregister(this);
		}
	}

	void runUntilCanceled() throws InterruptedException {

		while (!isCanceled()) {
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			Collection<Metric> metrics = databusMetricsQueue.poll(batchSize, maxIdleTime);
			log.debug("Back from polling databusMetricsQueue. metrics.size = {}",
					null == metrics ? "null" : metrics.size());
			// Check to see if we should down this writer entirely.
			log.debug("Checking for shutdown. lastWorkTime = {}; maxIdleTime = {}; sum = {}; currentTime ={}",
					lastWorkTime, maxIdleTime, lastWorkTime + maxIdleTime, System.currentTimeMillis());
			if (isNullOrEmpty(metrics) && // No records could be read from the
											// metrics queue
					lastWorkTime > 0 && // This thread has done work at least
										// once
					maxIdleTime > 0 && // The max idle time is set to something
										// meaningful
					System.currentTimeMillis() > lastWorkTime + maxIdleTime) // The
																				// max
																				// idle
																				// time
																				// has
																				// expired
			{
				log.info("Shutting down writer due to dearth of work");
				break;
			}

			/*
			 * If all the conditions were not met for shutting this writer down,
			 * we still might want to just abort this run if we didn't get any
			 * data from the metrics queue
			 */
			if (isNullOrEmpty(metrics)) {
				log.debug("No work to do, so checking again.");
				continue;
			}

			// We have some work to do, some process what we got from the
			// metrics queue

			try {
				processBatch(metrics);

			} catch (Exception e) {
				log.error("Exception during metric processing: {}", e.getMessage());
			}
		}
		log.debug("work canceled.");
	}

	private boolean isNullOrEmpty(Collection<Metric> metrics) {
		return null == metrics || metrics.isEmpty();
	}

	void processBatch(Collection<Metric> metrics) {
		long processed = 0;
		DatabusProducer producer = null;
		try {
			List<DatabusRecord<JsonNode, JsonNode>> producerRecords = metrics.stream()
					.map(m -> new DatabusRecord<JsonNode, JsonNode>(
							jsonConverter.convertValue(m.getMetric(), JsonNode.class),
							jsonConverter.convertValue(m, JsonNode.class)))
					.collect(Collectors.toList());

			producer = borrowDatabusProducer();
			if (producer != null) {
				producer.produce(producerRecords);
				processed = producerRecords.size();
				log.debug("Processed metrics: {}", processed);
			} else {
				log.error("Unable to retrieve producer!");
			}

		} catch (Exception e) {
			log.warn("Caught exception while processing metrics: {}", e.getMessage());

		} finally {
			returnDatabusProducer(producer);

			lastWorkTime = System.currentTimeMillis();
		}
	}

	private DatabusProducer borrowDatabusProducer() {
		DatabusProducer producer = null;

		try {
			producer = this.producerPool.borrowObject(this.topic);
			log.debug("Borrowed producer");
		} catch (Exception e) {
			log.error("Exception in retrieving producer: {}", e.getMessage());
			if (producer != null) {
				returnDatabusProducer(producer);
				producer = null;
			}

			throw new NoSuchElementException(e.toString());
		}

		return producer;
	}

	private void returnDatabusProducer(DatabusProducer producer) {
		//
		if (producer != null) {
			this.producerPool.returnObject(producer.getTopic(), producer);
			log.debug("Returned producer");
		}
	}


	public boolean isRunning() {
		return running;
	}

	private synchronized boolean isCanceled() {
		return canceled;
	}

	
	public synchronized void cancel() {
		log.info("Writer shutdown requested");
		this.canceled = true;
	}

}
