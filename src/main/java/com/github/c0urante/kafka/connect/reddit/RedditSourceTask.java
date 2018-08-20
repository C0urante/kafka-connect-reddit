/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit;

import com.github.c0urante.kafka.connect.reddit.Stream.Reddit;
import com.github.c0urante.kafka.connect.reddit.model.CommentSourceRecordConverter;
import com.github.c0urante.kafka.connect.reddit.model.PostSourceRecordConverter;
import com.github.c0urante.kafka.connect.reddit.model.SourceRecordConverter;
import com.github.c0urante.kafka.connect.reddit.version.Version;

import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.models.UniquelyIdentifiable;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class RedditSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RedditSourceTask.class);

    private List<SourceRecord> records;

    private Thread postsThread;
    private Thread commentsThread;

    @Override
    public void start(Map<String, String> props) {
        RedditSourceConnectorConfig config = new RedditSourceConnectorConfig(props);
        Reddit reddit = config.createClient();
        Stream<Submission> postsStream = reddit.posts(config.getPostSubreddits());
        Stream<Comment> commentsStream = reddit.comments(config.getCommentSubreddits());
        this.records = new ArrayList<>();

        Collection<Map<String, Object>> postPartitions = new HashSet<>();
        for (String postsSubreddit : config.getPostSubreddits()) {
            postPartitions.add(PostSourceRecordConverter.sourcePartition(postsSubreddit));
        }
        Collection<Map<String, Object>> commentPartitions = new HashSet<>();
        for (String commentsSubreddit : config.getCommentSubreddits()) {
            commentPartitions.add(CommentSourceRecordConverter.sourcePartition(commentsSubreddit));
        }

        Map<Map<String, Object>, Map<String, Object>> postOffsets =
                context.offsetStorageReader().offsets(postPartitions);
        Map<Map<String, Object>, Map<String, Object>> commentOffsets =
                context.offsetStorageReader().offsets(commentPartitions);

        if (postsStream != null) {
            postsThread = new Thread(new StreamReader<>(
                    postsStream,
                    new PostSourceRecordConverter(config.getPostsTopic()),
                    constructOriginalThingPredicate(
                            Submission::getCreated,
                            Submission::getSubreddit,
                            PostSourceRecordConverter::sourcePartition,
                            postOffsets
                    )
            ));
            postsThread.start();
            log.info("Starting posts thread reading subreddits {}", config.getPostSubreddits());
        }
        if (commentsStream != null) {
            commentsThread = new Thread(new StreamReader<>(
                    commentsStream,
                    new CommentSourceRecordConverter(config.getCommentsTopic()),
                    constructOriginalThingPredicate(
                            Comment::getCreated,
                            Comment::getSubreddit,
                            CommentSourceRecordConverter::sourcePartition,
                            commentOffsets
                    )
            ));
            commentsThread.start();
            log.info("Starting comments thread reading subreddits {}", config.getCommentSubreddits());
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> result;
        synchronized (records) {
            result = records;
            records = new ArrayList<>();
        }
        return result;
    }

    @Override
    public void stop() {
        if (postsThread != null) {
            postsThread.interrupt();
        }
        if (commentsThread != null) {
            commentsThread.interrupt();
        }

        if (postsThread != null) {
            try {
                postsThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while attempting to join posts thread");
            } finally {
                postsThread = null;
            }
        }

        if (commentsThread != null) {
            try {
                commentsThread.join();
            } catch (InterruptedException e) {
                log.warn("Interrupted while attempting to join comments thread");
            } finally {
                commentsThread = null;
            }
        }
    }

    @Override
    public String version() {
        return Version.get();
    }

    private class StreamReader<Thing extends UniquelyIdentifiable> implements Runnable {
        private final Stream<Thing> stream;
        private final SourceRecordConverter<Thing> recordConverter;
        private final Predicate<Thing> originalThing;

        public StreamReader(
                Stream<Thing> stream,
                SourceRecordConverter<Thing> recordConverter,
                Predicate<Thing> originalThing
        ) {
            this.stream = stream;
            this.recordConverter = recordConverter;
            this.originalThing = originalThing;
        }

        @Override
        public void run() {
            while (true) {
                Thing nextThing = stream.next();
                if (originalThing.test(nextThing)) {
                    SourceRecord record = recordConverter.convert(nextThing);
                    synchronized (records) {
                        records.add(record);
                    }
                }
            }
        }
    }

    private static <Thing> Predicate<Thing> constructOriginalThingPredicate(
            Function<Thing, Date> dateExtractor,
            Function<Thing, String> subredditExtractor,
            Function<String, Map<String, Object>> partitionExtractor,
            Map<Map<String, Object>, Map<String, Object>> offsets
    ) {
        return thing -> {
            String subreddit = subredditExtractor.apply(thing);
            Map<String, Object> partition = partitionExtractor.apply(subreddit);
            Map<String, Object> offset = offsets.get(partition);
            Long mostRecentThingTimestamp = offset != null ? (Long) offset.get("created") : null;
            return mostRecentThingTimestamp == null || mostRecentThingTimestamp < dateExtractor.apply(thing).getTime();
        };
    }
}
