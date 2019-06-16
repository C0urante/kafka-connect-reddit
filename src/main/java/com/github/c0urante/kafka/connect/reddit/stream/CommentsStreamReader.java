/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.stream;

import com.github.c0urante.kafka.connect.reddit.model.CommentSourceRecordConverter;
import net.dean.jraw.models.Comment;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class CommentsStreamReader extends StreamReader<Comment> {

    private final CommentSourceRecordConverter recordConverter;

    public CommentsStreamReader(
            Map<Map<String, Object>, Map<String, Object>> offsets,
            Stream<Comment> stream,
            List<String> subreddits,
            String topic
    ) {
        super(offsets, stream, "comment", subreddits);
        this.recordConverter = new CommentSourceRecordConverter(topic);
    }

    @Override
    protected SourceRecord convertThing(Comment comment) {
        return recordConverter.convert(comment);
    }

    @Override
    protected String subredditForThing(Comment comment) {
        return comment.getSubreddit();
    }

    @Override
    protected Map<String, Object> partitionForSubreddit(String subreddit) {
        return recordConverter.sourcePartition(subreddit);
    }

    @Override
    protected Date dateForThing(Comment comment) {
        return comment.getCreated();
    }
}
