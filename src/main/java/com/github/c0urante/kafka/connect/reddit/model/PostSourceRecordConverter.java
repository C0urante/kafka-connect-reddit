/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.model;

import net.dean.jraw.JrawUtils;
import net.dean.jraw.models.Submission;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.Map;

public class PostSourceRecordConverter implements SourceRecordConverter<Submission> {

    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("subreddit", Schema.STRING_SCHEMA)
            .build();


    private final String topic;

    public PostSourceRecordConverter(String topic) {
        this.topic = topic;
    }

    @Override
    public SourceRecord convert(Submission submission) {
        return new SourceRecord(
                sourcePartition(submission),
                sourceOffset(submission),
                topic,
                KEY_SCHEMA,
                convertKey(submission),
                null,
                convertValue(submission)
        );
    }

    public static Map<String, Object> sourcePartition(String subreddit) {
        return Collections.singletonMap("posts-subreddit", subreddit);
    }

    private static Map<String, ?> sourcePartition(Submission submission) {
        return sourcePartition(submission.getSubreddit());
    }

    private static  Map<String, ?> sourceOffset(Submission submission) {
        return Collections.singletonMap("created", submission.getCreated().getTime());
    }

    private static Struct convertKey(Submission submission) {
        Struct result = new Struct(KEY_SCHEMA);
        result.put("subreddit", submission.getSubreddit());
        return result;
    }

    private static Object convertValue(Submission submission) {
        return Submission.jsonAdapter(JrawUtils.moshi).toJsonValue(submission);
    }
}

/*
    Submission{
        score=5,
        body=null,
        saved=false,
        stickied=false,
        author=suckersponge,
        authorFlairText=null,
        archived=false
        gildable=true,
        created=Tue Aug 07 17:47:09 PDT 2018,
        contestMode=false,
        distinguished=NORMAL,
        domain=imgur.com,
        edited=null,
        embeddedMedia=null,
        fullName=t3_95hd9w,
        gilded=0,
        hidden=false,
        scoreHidden=true,
        id=95hd9w,
        selfPost=false,
        linkFlairText=null,
        linkFlairCssClass=null,
        locked=false,
        nsfw=false,
        permalink=/r/aww/comments/95hd9w/come_get_your_cat_he_looks_dead/,
        postHint=link,
        preview=SubmissionPreview{
            images=[
                ImageSet{
                    source=Variation{
                        url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?s=7a15e1a9d911a5a28a6023511ee57145,
                        width=3603,
                        height=1640
                    },
                    resolutions=[
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=108&s=caa8db3e34e235155fce82ec8db7abf2,
                            width=108,
                            height=49
                        },
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=216&s=d370fa4d9cf111d0130e0d7839d82ddf,
                            width=216,
                            height=98
                        },
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=320&s=807d2304f89e203aeb59d2bdfc9be683,
                            width=320,
                            height=145
                        },
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=640&s=ddd7af7df8ea6ff613543c6d8f75d1a5,
                            width=640,
                            height=291
                        },
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=960&s=1daf665a7a7211215738e88127dbdfdf,
                            width=960,
                            height=436
                        },
                        Variation{
                            url=https://i.redditmedia.com/wVgB5SBIKo3WwYmes-RHnrdeCTdCFr2lNGqUxeHGuCM.jpg?fit=crop&crop=faces%2Centropy&arh=2&w=1080&s=edc4b61b36ada6f617536df2adb1d5e5,
                            width=1080,
                            height=491
                        }
                    ],
                    id=p3qm-MicxDjsepruqtYv8bDdzTyRm2LgRW6QUKLjvXg
                }
            ],
            enabled=false
        },
        quarantine=false,
        reports=null,
        selfText=,
        spam=false,
        spoiler=false,
        subreddit=aww,
        subredditFullName=t5_2qh1o,
        suggestedSort=null,
        thumbnail=https://a.thumbs.redditmedia.com/GjbNXnA1ZFqlbaPB78OJ7Vi3OMRUm9iFueA2e6Kh-P0.jpg,
        title="Come get your cat, he looks dead",
        url=https://imgur.com/HLYLfDG,
        visited=false,
        removed=false,
        vote=NONE,
        commentCount=1
    }
 */
