#!/bin/bash

# We're going to override the subreddit, comments, and broker if we have an environment variable set

[[ -v SUBREDDIT ]] && sed -ir 's/^posts\.subreddits=.*$/posts.subreddits='"$SUBREDDIT/" kafka-connect-reddit.properties
[[ -v COMMENTS_SUBREDDIT ]] && echo "comments.subreddits=$COMMENTS_SUBREDDIT" >> /kafka-connect-reddit/kafka-connect-reddit-source.properties
[[ -v KAFKA_SERVER ]] && echo "bootstrap.servers=$KAFKA_SERVER" >> /kafka-connect-reddit/connect-standalone.properties

echo "plugin.path=/kafka-connect-reddit/install/" >> /kafka-connect-reddit/connect-standalone.properties

/usr/bin/connect-standalone /kafka-connect-reddit/connect-standalone.properties /kafka-connect-reddit/kafka-connect-reddit-source.properties
