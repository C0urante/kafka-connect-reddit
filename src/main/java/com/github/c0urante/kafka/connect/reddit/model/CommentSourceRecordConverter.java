/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.model;

import net.dean.jraw.JrawUtils;
import net.dean.jraw.models.Comment;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.Map;

public class CommentSourceRecordConverter implements SourceRecordConverter<Comment> {

    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("subreddit", Schema.STRING_SCHEMA)
            .build();


    private final String topic;

    public CommentSourceRecordConverter(String topic) {
        this.topic = topic;
    }

    @Override
    public SourceRecord convert(Comment comment) {
        return new SourceRecord(
                sourcePartition(comment),
                sourceOffset(comment),
                topic,
                KEY_SCHEMA,
                convertKey(comment),
                null,
                convertValue(comment)
        );
    }

    public static Map<String, Object> sourcePartition(String subreddit) {
        return Collections.singletonMap("comments-subreddit", subreddit);
    }

    private static Map<String, ?> sourcePartition(Comment comment) {
        return sourcePartition(comment.getSubreddit());
    }

    private static Map<String, ?> sourceOffset(Comment comment) {
        return Collections.singletonMap("created", comment.getCreated().getTime());
    }

    private static Struct convertKey(Comment comment) {
        Struct result = new Struct(KEY_SCHEMA);
        result.put("subreddit", comment.getSubreddit());
        return result;
    }

    private Object convertValue(Comment comment) {
        return Comment.jsonAdapter(JrawUtils.moshi).toJsonValue(comment);
    }
}

/*
Comment{
    gilded=0,
    id=e3svktv,
    score=1,
    saved=false,
    stickied=false,
    subreddit=aww,
    archived=false,
    author=Xuvial,
    authorFlairText=null,
    gildable=true,
    controversiality=0,
    created=Tue Aug 07 19:19:32 PDT 2018,
    distinguished=NORMAL,
    edited=Tue Aug 07 19:27:13 PDT 2018,
    fullName=t1_e3svktv,
    body=> They can't really defend themselves well because everything else is so much bigger than them\n\nDogs don't care how big or small they are. It's all about body language, signals and dominance/comment. It's quite common to see tiny dogs/cats dominating and subduing an animal 10x bigger than themselves.\n\nI've found that misbehaving Chihuahuas have almost nothing to do with the breed/personality, and everything to do with the owner. Especially the kinds of owners that toydog breeds attract. A tremendous number of Chihuahua owners tend to spoil their pup, neglect basic discipline, and shelter them from any kind of consequence for their actions. This almost always results in a Chihuahua who has no concept of boundaries, tends to run the household, and assumes dominance over everything and everyone.\n\nChihuahua owners also tend to carry their dogs high off the ground (making them feel even *more* dominant), and they also transfer their fears and insecurities over to the dog. \n\nNet result = unstable, nervous, or aggressive Chihuahua. This applies to all dog breeds mind you. It's just the most severe with toydog breeds because of the kinds of owners they attract.,
    replies=Listing{
        nextName=null,
        children=[]
    },
    parentFullName=t1_e3sovdx,
    commentFullName=t3_95en27,
    commentTitle=Alpha male defending his territory,
    url=https://v.redd.it/hejbia6s0qe11,
    subredditFullName=t5_2qh1o,
    subredditType=PUBLIC,
    scoreHidden=false,
    vote=NONE
}
*/
