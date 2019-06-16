/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.stream;

import net.dean.jraw.ApiException;
import net.dean.jraw.models.UniquelyIdentifiable;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StreamReader<Thing extends UniquelyIdentifiable> implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(StreamReader.class);

    private final Map<Map<String, Object>, Map<String, Object>> offsets;
    private final Stream<Thing> stream;
    private final String asString;

    private final AtomicBoolean running;
    private final List<SourceRecord> records;

    private Thread readerThread;

    protected abstract SourceRecord convertThing(Thing thing);
    protected abstract String subredditForThing(Thing thing);
    protected abstract Map<String, Object> partitionForSubreddit(String subreddit);
    protected abstract Date dateForThing(Thing thing);

    public StreamReader(
            Map<Map<String, Object>, Map<String, Object>> offsets,
            Stream<Thing> stream,
            String thingType,
            List<String> subreddits
    ) {
        this.offsets = offsets;
        this.stream = stream;
        this.asString = String.format(
                "%s stream reader (subreddits: %s)",
                thingType,
                String.join(",", subreddits)
        );

        this.running = new AtomicBoolean(true);
        this.records = new ArrayList<>();

        this.readerThread = null;
    }

    @Override
    public String toString() {
        return asString;
    }

    @Override
    public void run() {
        log.info("Started thread for {}", this);
        while (running.get()) {
            try {
                Thing nextThing = retrieveNextThing();
                if (isOriginalThing(nextThing)) {
                    SourceRecord record = convertThing(nextThing);
                    synchronized (records) {
                        records.add(record);
                    }
                }
            } catch (Throwable t) {
                log.error("Error while reading from {}", this, t);
                throw t;
            }
        }
    }

    @Override
    public void close() {
        this.running.set(false);

        if (readerThread == null) {
            log.warn("close() invoked but reader thread already halted; ignoring");
            return;
        }

        log.debug("Interrupting thread {}", readerThread);
        readerThread.interrupt();

        log.debug("Attempting to join thread {}", readerThread);
        try {
            readerThread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while attempting to join thread {}", readerThread);
        }

        readerThread = null;
    }

    public void startReaderThread() {
        readerThread = new Thread(this);
        readerThread.setDaemon(true);
        readerThread.setName(toString());
        readerThread.start();
    }

    private Thing retrieveNextThing() {
        try {
            return stream.next();
        } catch (ApiException e) {
            if ("401".equals(e.getCode())) {
                log.debug(
                        "Encountered 401 response while reading from {}. " +
                                "This is likely due to a token refresh issue in the underlying Reddit client " +
                                "library; retrying now as the token should be refreshed correctly this time.",
                        this,
                        e
                );
                return stream.next();
            } else {
                throw e;
            }
        }
    }

    private boolean isOriginalThing(Thing thing) {
        String subreddit = subredditForThing(thing);
        Map<String, Object> partition = partitionForSubreddit(subreddit);
        Map<String, Object> offset = offsets.get(partition);
        Long mostRecentThingTimestamp = offset != null ? (Long) offset.get("created") : null;
        return mostRecentThingTimestamp == null || mostRecentThingTimestamp < dateForThing(thing).getTime();
    }

    public List<SourceRecord> pollRecords() {
        List<SourceRecord> result;
        synchronized (records) {
            if (records.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = new ArrayList<>(records);
                records.clear();
            }
        }
        return result;
    }
}
