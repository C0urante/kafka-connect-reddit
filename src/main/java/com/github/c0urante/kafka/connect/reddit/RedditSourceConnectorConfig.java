/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit;

import com.github.c0urante.kafka.connect.reddit.stream.Reddit;
import net.dean.jraw.pagination.Paginator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedditSourceConnectorConfig extends AbstractConfig {

    public static final String OAUTH_CLIENT_ID = "_7fRyo80H7qdFA";


    public static final String COMMENTS_SUBREDDITS_CONFIG = "comments.subreddits";
    public static final String COMMENTS_SUBREDDITS_DEFAULT = "";
    public static final String COMMENTS_SUBREDDITS_DOC = "A list of subreddits to read comments from";

    public static final String COMMENTS_TOPIC_CONFIG = "comments.topic";
    public static final String COMMENTS_TOPIC_DEFAULT = "reddit-comments";
    public static final String COMMENTS_TOPIC_DOC = "The name of the topic to write comments to";

    public static final String POSTS_SUBREDDITS_CONFIG = "posts.subreddits";
    public static final String POSTS_SUBREDDITS_DEFAULT = "";
    public static final String POSTS_SUBREDDITS_DOC = "A list of subreddits to read posts from";

    public static final String POSTS_TOPIC_CONFIG = "posts.topic";
    public static final String POSTS_TOPIC_DEFAULT = "reddit-posts";
    public static final String POSTS_TOPIC_DOC = "The name of the topic to write posts to";


    public static final String CONSUMPTION_LIMIT_CONFIG = "consumption.limit";
    public static final String CONSUMPTION_LIMIT_DEFAULT = Integer.toString(Paginator.RECOMMENDED_MAX_LIMIT, 10);
    public static final String CONSUMPTION_LIMIT_DOC = "The maximum number of Things to consume per API call";


    public static final String REDDIT_LOG_HTTP_REQUESTS_CONFIG = "reddit.log.http.requests";
    public static final String REDDIT_LOG_HTTP_REQUESTS_DEFAULT = "false";
    public static final String REDDIT_LOG_HTTP_REQUESTS_DOC = "Whether to log HTTP requests made to Reddit";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    COMMENTS_SUBREDDITS_CONFIG,
                    ConfigDef.Type.LIST,
                    COMMENTS_SUBREDDITS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    COMMENTS_SUBREDDITS_DOC
            ).define(
                    POSTS_SUBREDDITS_CONFIG,
                    ConfigDef.Type.LIST,
                    POSTS_SUBREDDITS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    POSTS_SUBREDDITS_DOC
            ).define(
                    CONSUMPTION_LIMIT_CONFIG,
                    ConfigDef.Type.INT,
                    CONSUMPTION_LIMIT_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.MEDIUM,
                    CONSUMPTION_LIMIT_DOC
            ).define(
                    COMMENTS_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    COMMENTS_TOPIC_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    COMMENTS_TOPIC_DOC
            ).define(
                    POSTS_TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    POSTS_TOPIC_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    POSTS_TOPIC_DOC
            ).define(
                    REDDIT_LOG_HTTP_REQUESTS_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    REDDIT_LOG_HTTP_REQUESTS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REDDIT_LOG_HTTP_REQUESTS_DOC
            );

    private final List<String> postSubreddits;
    private final List<String> commentSubreddits;
    private final String postsTopic;
    private final String commentsTopic;

    public RedditSourceConnectorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);

        this.postSubreddits = getList(POSTS_SUBREDDITS_CONFIG);
        this.commentSubreddits = getList(COMMENTS_SUBREDDITS_CONFIG);
        this.postsTopic = getString(POSTS_TOPIC_CONFIG);
        this.commentsTopic = getString(COMMENTS_TOPIC_CONFIG);
    }

    public Reddit createClient() {
        return new Reddit(
                OAUTH_CLIENT_ID,
                getInt(CONSUMPTION_LIMIT_CONFIG),
                getBoolean(REDDIT_LOG_HTTP_REQUESTS_CONFIG)
        );
    }

    public List<String> getPostSubreddits() {
        return new ArrayList<>(postSubreddits);
    }

    public List<String> getCommentSubreddits() {
        return new ArrayList<>(commentSubreddits);
    }

    public Set<String> getAllSubreddits() {
        Set<String> result = new HashSet<>();
        result.addAll(postSubreddits);
        result.addAll(commentSubreddits);
        return result;
    }

    public String getPostsTopic() {
        return postsTopic;
    }

    public String getCommentsTopic() {
        return commentsTopic;
    }

    public static void main(String[] args) throws FileNotFoundException {
        OutputStream out;
        if (args.length == 1 && !args[0].equals("-")) {
            out = new FileOutputStream(args[0]);
        } else if (args.length <= 1) {
            out = System.out;
        } else {
            System.err.printf("Usage: %s [<file>]%n", RedditSourceConnectorConfig.class.getSimpleName());
            System.exit(1);
            return; // Only here to make the compiler happy
        }

        try (PrintWriter writer = new PrintWriter(out)) {
            writer.write(CONFIG_DEF.toEnrichedRst());
            writer.flush();
        }
    }
}

