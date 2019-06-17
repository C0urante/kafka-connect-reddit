/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit;

import com.github.c0urante.kafka.connect.reddit.stream.CommentsStreamReader;
import com.github.c0urante.kafka.connect.reddit.stream.PostsStreamReader;
import com.github.c0urante.kafka.connect.reddit.stream.Reddit;
import com.github.c0urante.kafka.connect.reddit.model.CommentSourceRecordConverter;
import com.github.c0urante.kafka.connect.reddit.model.PostSourceRecordConverter;
import com.github.c0urante.kafka.connect.reddit.stream.StreamReader;
import com.github.c0urante.kafka.connect.reddit.version.Version;

import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedditSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RedditSourceTask.class);

    private List<StreamReader<?>> streamReaders;

    @Override
    public void start(Map<String, String> props) {
        RedditSourceConnectorConfig config = new RedditSourceConnectorConfig(props);
        Reddit reddit = config.createClient();
        Stream<Submission> postsStream = reddit.posts(config.getPostSubreddits());
        Stream<Comment> commentsStream = reddit.comments(config.getCommentSubreddits());

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

        streamReaders = new ArrayList<>();

        if (postsStream != null) {
            PostsStreamReader postsReader = new PostsStreamReader(
                    postOffsets,
                    postsStream,
                    config.getPostSubreddits(),
                    config.getPostsTopic()
            );
            postsReader.startReaderThread();
            streamReaders.add(postsReader);
        }
        if (commentsStream != null) {
            CommentsStreamReader commentsReader = new CommentsStreamReader(
                    commentOffsets,
                    commentsStream,
                    config.getCommentSubreddits(),
                    config.getCommentsTopic()
            );
            commentsReader.startReaderThread();
            streamReaders.add(commentsReader);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (streamReaders == null) {
            log.warn("poll() invoked after task has been stopped; ignoring");
            return Collections.emptyList();
        }
        return streamReaders.stream().map(StreamReader::pollRecords).flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        streamReaders.forEach(StreamReader::close);
        streamReaders = null;
    }

    @Override
    public String version() {
        return Version.get();
    }
}
