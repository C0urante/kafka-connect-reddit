/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.stream;

import com.github.c0urante.kafka.connect.reddit.model.PostSourceRecordConverter;
import net.dean.jraw.models.Submission;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class PostsStreamReader extends StreamReader<Submission> {

    private final PostSourceRecordConverter recordConverter;

    public PostsStreamReader(
            Map<Map<String, Object>, Map<String, Object>> offsets,
            Stream<Submission> stream,
            List<String> subreddits,
            String topic
    ) {
        super(offsets, stream, "posts", subreddits);
        this.recordConverter = new PostSourceRecordConverter(topic);
    }

    @Override
    protected SourceRecord convertThing(Submission submission) {
        return recordConverter.convert(submission);
    }

    @Override
    protected String subredditForThing(Submission submission) {
        return submission.getSubreddit();
    }

    @Override
    protected Map<String, Object> partitionForSubreddit(String subreddit) {
        return recordConverter.sourcePartition(subreddit);
    }

    @Override
    protected Date dateForThing(Submission submission) {
        return submission.getCreated();
    }
}
