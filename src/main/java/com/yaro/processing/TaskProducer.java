package com.yaro.processing;

import com.google.common.collect.Lists;
import com.yaro.processing.db.DAO;
import com.yaro.processing.domain.Identifiable;
import com.yaro.processing.processor.TaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author ysperetz
 */
public class TaskProducer<T extends Identifiable> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskProducer.class);
    private final TaskProcessor<T> processor;
    private final DAO<T> dao;
    private final ExecutorService pool;
    private volatile boolean finished = false;
    private final int batchSize;
    private final JobConfiguration config;
    private long offset = 0;

    public TaskProducer(ExecutorService pool, TaskProcessor processor,
                        DAO<T> dao, JobConfiguration config) {
        this.pool = pool;
        this.processor = processor;
        batchSize = config.getBatchSize();
        this.config = config;
        offset = config.getOffset();
        this.dao = dao;
    }

    @Override
    public void run() {
        ErrorHandler handler = new ErrorHandler(config);

        LOGGER.info("Start offset: " + offset);
        try {

            while (true) {
                if (Thread.interrupted()) {
                    break;
                }
                List<T> items = null;
                try {
                    items = dao.read(offset);
                }
                catch (SQLException e) {
                    LOGGER.error("TaskProducer was not able to read items from DAO.", e);
                }
                if (items == null || items.isEmpty()) {
                    break;
                }

                // Update offset to be the last itemId from result.
                offset = items.get(items.size() - 1).getId();
                LOGGER.info("currentOffset = {}", offset);

                List<T> filteredTtems = items.stream()
                        .filter(this::isMyPartition)
                        .collect(Collectors.<T>toList());
                if (filteredItems.isEmpty()) {
                    //We loaded items at this point but all of them are filtered out. Load another portion.
                    continue;
                }

                List<List<T>> partitions = Lists.partition(filteredItems, batchSize);
                for (List<T> partition : partitions) {
                    pool.execute(() -> {
                            long startTime = System.currentTimeMillis();
                            try {
                                List<T> processedList = partition.stream()
                                        .map(processor::process)
                                        .filter(Objects::nonNull)
                                        .collect(toList());
                                if (processedList != null && !processedList.isEmpty()) {
                                    LOGGER.warn("Average processing time per item (ms)",
                                            (System.currentTimeMillis() - startTime) / processedList.size());
                                    dao.update(processedList);
                                }
                            }
                            catch (SQLException e) {
                                handler.taskException(e);
                            }
                        });
                }
            }
        }
        catch (Exception e) {
            LOGGER.error("Task producer stopped.", e);
        }
        finally {
            finished = true;
        }
    }

    private boolean isMyPartition(T item) {
        return isSamePartition(item, config.getPartitionNumber(), config.getTotalPartitions());
    }

    static boolean isSamePartition(Identifiable item, int partitionNumber, int totalPartitions) {
        return totalPartitions - partitionNumber == item.getId() % totalPartitions;
    }

    public boolean isFinished() {
        return finished;
    }
}
