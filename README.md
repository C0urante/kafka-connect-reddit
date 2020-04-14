# Kafka Connect Reddit

A source connector for reading [Reddit] posts and comments into
[Apache Kafka], via the [Kafka Connect] framework

1. [Overview](#overview)
1. [Installation](#installation)
1. [Configuration](#configuration)
1. [Quickstart](#quickstart)
1. [Offset Tracking](#offset-tracking)
1. [Data Format](#data-format)
1. [Issue Tracking](#issue-tracking)
1. [TODO](#todo)

## Overview

The connector consumes posts and comments from Reddit. As is
demonstrated in the
[sample config file](config/kafka-connect-reddit-source.properties),
it's possible to read comments from one collection of subreddits and
posts from a different collection, and [r/all](reddit.com/r/all) can
be read from for either.

Separate topics are used for posts and comments.

[JRAW] is used to continuously stream new posts and comments; many
thanks go to [Matt Dean](https://github.com/mattbdean) for writing such
an excellent library.

## Installation


### Via Confluent Hub

The connector is available on
[Confluent Hub](https://www.confluent.io/hub/C0urante/kafka-connect-reddit). Instructions on how to
download it and install it onto your Kafka Connect worker(s) can be found there.

### Local build

The connector can be built locally by running the following command:

```bash
mvn package
```

and then copying and unzipping the zip archive generated in the `target/components/packages`
directory onto the plugin path or classpath for your Kafka Connect worker(s).

## Running standalone in docker

To run this in standalone-mode via a docker container. (This brings up the environment + the connect-standalone):

```bash
#Build the project 
mvn clean package

# Build the docker container 
docker build -t redditconnector .

# Run 
docker run --env-file reddit.env -it redditconnector

```

The environment file that was used looks like this: (To configure the subreddit, comment location, and where your broker can be reached)

```bash
SUBREDDIT=chicagohelicopters
COMMENTS_SUBREDDIT=chicagohelicopters
KAFKA_SERVER=<kafka hostname>:9092
```

Note: Change the kafka host name. If the variable in the file or docker run arguements is not present then it will default to the following values:

 * SUBREDDIT - all
 * COMMENTS_SUBREDDIT - all
 * KAFKA_SERVER - localhost:9092


## Configuration

[Docs](docs/source-connector-config.md)

[Example](config/kafka-connect-reddit-source.properties)

## Quickstart

Assumptions:

- Maven 3+ is installed
- Zookeeper is running and listening on localhost:2181
- Kafka is running and listening on localhost:9092
- Current directory is the root of the repository

```bash
# Build the project
mvn clean install

# Create the topics that the connector will populate
kafka-topics --zookeeper localhost:2181 --create --topic reddit-comments --partitions 3 --replication-factor 1
kafka-topics --zookeeper localhost:2181 --create --topic reddit-posts --partitions 3 --replication-factor 1

# Run the connector
connect-standalone config/connect-standalone.properties config/kafka-connect-reddit-source.properties
```

  

## Offset Tracking

Offsets are tracked via timestamp on a per-subreddit basis. If the
connector is killed and restarted, it will ignore any posts/comments
whose timestamps are less recent than the most-recently-consumed
timestamp for the given subreddit and content type. This should help
with deduplication.

However, due to the nature of the Reddit API, JRAW, and
what currently seem to be acceptable costs of failure, content may be
irretrievably lost during this period. Upon startup, the connector will
only ask for `<consumption.limit>` of the most recent posts/comments; if
more than that number have been created while it was down, they will not
be read or sent to Kafka.

## Data Format

The key used for each record is the subreddit it came from; this means
that with key-based partitioning, chronological ordering is guaranteed
within individual subreddits, posts, and comment threads.

Example comment value (serialized to Json via the [JsonConverter]):

```json
{
  "gilded": 0,
  "id": "e4hqxoz",
  "score": 1,
  "saved": false,
  "stickied": false,
  "subreddit": "malaysia",
  "archived": false,
  "author": "luxollidd",
  "author_flair_text": "Deus Vult!",
  "can_gild": true,
  "controversiality": 0,
  "created_utc": 1534727422,
  "name": "t1_e4hqxoz",
  "body": "Rockets are expensive.\n\nI dont think it should be our nation's priority for now.",
  "replies": {
    "kind": "Listing",
    "data": {
      "children": []
    }
  },
  "parent_id": "t3_98je8o",
  "link_id": "t3_98je8o",
  "link_title": "Malaysian National Space Agency (ANGKASA)",
  "link_url": "https://www.reddit.com/r/malaysia/comments/98je8o/malaysian_national_space_agency_angkasa/",
  "subreddit_id": "t5_2qh8b",
  "subreddit_type": "public",
  "score_hidden": false
}
```

Example post value (serialized to Json via the [JsonConverter]):

```json
{
  "score": 1,
  "saved": false,
  "stickied": false,
  "author": "butteredtoastisgreat",
  "archived": false,
  "can_gild": true,
  "created_utc": 1534727418,
  "contest_mode": false,
  "domain": "self.DebateTrade",
  "name": "t3_98pbjf",
  "gilded": 0,
  "hidden": false,
  "hide_score": true,
  "id": "98pbjf",
  "is_self": true,
  "link_flair_text": "PF",
  "link_flair_css_class": "pf",
  "locked": false,
  "over_18": false,
  "permalink": "/r/DebateTrade/comments/98pbjf/pfhstrong_neg_case_w_overfishing_link_cards_or/",
  "quarantine": false,
  "selftext": "",
  "spam": false,
  "spoiler": false,
  "subreddit": "DebateTrade",
  "subreddit_id": "t5_2zi75",
  "thumbnail": "self",
  "title": "[PF]H:Strong Neg Case W :OVerfishing link cards, or full contention",
  "url": "https://www.reddit.com/r/DebateTrade/comments/98pbjf/pfhstrong_neg_case_w_overfishing_link_cards_or/",
  "visited": false,
  "removed": false,
  "num_comments": 0
}
```

## Issue Tracking

Issues are tracked on GitHub. If there's a problem you're running into
with the connector or a feature missing that you'd like to see, please
open an issue.

If there's a small bug or typo that you'd like to fix, feel free to open
a PR without filing an issue first and tag @C0urante for review.

## TODO

- [x] Publish to [Confluent Hub]
- [ ] Support reverse-chronological consumption
- [ ] Sink connector
- [ ] (In Docker) Replace variables in the prioperties files rather than append 

PRs welcome and encouraged!

[Kafka Connect]: https://docs.confluent.io/current/connect
[Apache Kafka]: https://kafka.apache.org
[Reddit]: https://www.redditinc.com/
[JRAW]: https://github.com/mattbdean/JRAW
[JsonConverter]: https://github.com/apache/kafka/blob/2.0.0/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java
[Confluent Hub]: https://confluent.io/hub
