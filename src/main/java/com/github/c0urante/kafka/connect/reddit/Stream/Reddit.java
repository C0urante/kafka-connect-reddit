/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.Stream;

import com.github.c0urante.kafka.connect.reddit.version.Version;
import net.dean.jraw.ApiException;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.OkHttpNetworkAdapter;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.models.SubredditSort;
import net.dean.jraw.models.UniquelyIdentifiable;
import net.dean.jraw.oauth.Credentials;
import net.dean.jraw.oauth.OAuthHelper;
import net.dean.jraw.pagination.Paginator;
import net.dean.jraw.pagination.Stream;
import net.dean.jraw.references.SubredditReference;

import java.util.List;
import java.util.UUID;

public class Reddit {

    private static final UserAgent userAgent = new UserAgent(
            "kafka",
            "com.github.c0urante.kafka.connect.reddit",
            Version.get(),
            "C0urante"
    );

    private static final UUID DEVICE_ID = UUID.randomUUID();

    private final int limit;
    private final RedditClient reddit;

    public Reddit(String oAuthClientId, int limit, boolean logHttpRequests) {
        this.limit = limit;

        reddit = OAuthHelper.automatic(
                new OkHttpNetworkAdapter(userAgent),
                Credentials.userlessApp(oAuthClientId, DEVICE_ID)
        );
        reddit.setLogHttp(logHttpRequests);
    }

    public boolean canAccessSubreddit(String subreddit) {
        try {
            reddit.subreddit(subreddit).about();
            return true;
        } catch (ApiException e) {
            return false;
        }
    }

    public Stream<Comment> comments(List<String> subreddits) {
        SubredditReference multireddit = subreddits(subreddits);
        return multireddit != null
                ? stream(multireddit.comments().limit(limit))
                : null;
    }

    public Stream<Submission> posts(List<String> subreddits) {
        SubredditReference multireddit = subreddits(subreddits);
        return multireddit != null
                ? stream(multireddit.posts().limit(limit).sorting(SubredditSort.NEW))
                : null;
    }

    private <Thing extends UniquelyIdentifiable> Stream<Thing> stream(Paginator.Builder<Thing> builder) {
        return builder.build().stream();
    }

    private SubredditReference subreddits(List<String> subreddits) {
        switch (subreddits.size()) {
            case 0:
                return null;
            case 1:
                return reddit.subreddit(subreddits.get(0));
            case 2:
                return reddit.subreddits(subreddits.get(0), subreddits.get(0));
            default: {
                String[] others = subreddits.subList(2, subreddits.size()).toArray(new String[0]);
                return reddit.subreddits(subreddits.get(0), subreddits.get(1), others);
            }
        }
    }
}
