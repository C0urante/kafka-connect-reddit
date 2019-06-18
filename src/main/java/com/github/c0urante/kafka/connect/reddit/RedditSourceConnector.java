/*
     Copyright © 2018 - 2019 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit;

import com.github.c0urante.kafka.connect.reddit.stream.Reddit;
import com.github.c0urante.kafka.connect.reddit.version.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedditSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(RedditSourceConnector.class);

    private RedditSourceConnectorConfig config;
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        configProps = new HashMap<>(props);
        config = new RedditSourceConnectorConfig(props);

        Reddit reddit = config.createClient();
        // Subreddits can be created/deleted/made public/made private while the connector is
        // running, so we only log a warning if we can't access a subreddit here instead of raising
        // an exception
        for (String subreddit : config.getAllSubreddits()) {
            if (!reddit.canAccessSubreddit(subreddit)) {
                log.warn(
                        "Unable to access subreddit r/{}; it may not exist, or be set to private",
                        subreddit
                );
            }
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedditSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<List<String>> partitionedPostSubreddits =
                ConnectorUtils.groupPartitions(config.getPostSubreddits(), maxTasks);
        List<List<String>> partitionedCommentSubreddits =
                ConnectorUtils.groupPartitions(config.getCommentSubreddits(), maxTasks);
        int numTasks = Math.min(config.getPostSubreddits().size() + config.getCommentSubreddits().size(), maxTasks);

        List<Map<String, String>> result = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            result.add(taskConfig);
            taskConfig.remove(RedditSourceConnectorConfig.POSTS_SUBREDDITS_CONFIG);
            taskConfig.remove(RedditSourceConnectorConfig.COMMENTS_SUBREDDITS_CONFIG);
        }

        int task = 0;
        for (List<String> postSubreddits : partitionedPostSubreddits) {
            if (postSubreddits.isEmpty()) {
                continue;
            }

            result.get(task).put(
                    RedditSourceConnectorConfig.POSTS_SUBREDDITS_CONFIG,
                    String.join(",", postSubreddits)
            );
            task = (task + 1) % numTasks;
        }
        for (List<String> commentSubreddits : partitionedCommentSubreddits) {
            if (commentSubreddits.isEmpty()) {
                continue;
            }

            result.get(task).put(
                    RedditSourceConnectorConfig.COMMENTS_SUBREDDITS_CONFIG,
                    String.join(",", commentSubreddits)
            );
            task = (task + 1) % numTasks;
        }
        return result;
    }

    @Override
    public void stop() {
        config = null;
    }

    @Override
    public ConfigDef config() {
        return RedditSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.get();
    }
}
