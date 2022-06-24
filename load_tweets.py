import json
from py2neo.ogm import Repository, Model, Property, RelatedTo, RelatedFrom
import time

DUMP_FILE_PATH = "may2022_uy_test.json"

# Connect the repository to the DB
db_url = 'neo4j@bolt://neo4j@localhost:7687'
db_pass = ''

repo = Repository(db_url, password = db_pass)

users_dict = {}
hashtag_dict = {}
tweets_dict = {}
departments_dict = {}
countries_dict = {}
total_load_time = 0

class User(Model):
  __primarykey__ = "id"

  id = Property()
  name = Property()
  username = Property()
  followers = Property()
  following = Property()
  tweet_count = Property()

  tweeted = RelatedTo("Tweet")
  retweeted = RelatedTo("Retweet")
  mentions = RelatedFrom("Tweet", "MENTION")

class Tweet(Model):
  __primarykey__ = "id"

  id = Property()
  text = Property()
  created_at = Property()
  conversation_id = Property()
  department = Property()
  country = Property()
  retweet_count = Property()
  reply_count = Property()
  like_count = Property()
  quote_count = Property()

  author = RelatedFrom(User, "TWEETED")
  retweets = RelatedFrom("Retweet", "RT")
  quote = RelatedTo("Tweet")
  reply = RelatedTo("Tweet")
  mentions = RelatedTo(User)
  tag = RelatedTo("Hashtag")

class Retweet(Model):
  __primarykey__ = "id"

  id = Property()
  created_at = Property()
  conversation_id = Property()
  department = Property()
  country = Property()
  retweet_count = Property()
  reply_count = Property()
  like_count = Property()
  quote_count = Property()

  author = RelatedFrom(User, "RETWEETED")
  rt = RelatedTo(Tweet)

class Hashtag(Model):
  __primarykey__ = "tag"

  tag = Property()

  tweets = RelatedFrom(Tweet, "TAG")


# Function to read batches of 100 tweets from a dump file and load data to the graph database 

def read_data_and_load(dump_file_path, load_fn):
  with open(dump_file_path) as raw_file:
    raw_text = raw_file.read()
    lines = raw_text.split('\n')
    processed_tweets = 0
    lost_retweets = 0
    lost_quotes = 0
    lost_replies = 0
    total_time = 0
    total_lines = len(lines)
    line_num = 0
    for line in lines:
      if len(line) > 0:
        raw_data_batch = json.loads(line)
        start = time.perf_counter()
        processed_tweets_, lost_retweets_, lost_quotes_, lost_replies_ = load_fn(raw_data_batch)
        processed_tweets += processed_tweets_
        lost_retweets += lost_retweets_
        lost_quotes += lost_quotes_
        lost_replies += lost_replies_
        end = time.perf_counter()
        batch_time = end-start
        total_time += batch_time      
        line_num += 1
        print(f"Batch load time: {batch_time} - Progress {line_num / total_lines * 100}")
    print(f"Processed {processed_tweets} tweets")
    print(f"Lost Retweets: {lost_retweets}")
    print(f"Lost Quotes: {lost_quotes}")
    print(f"Lost Replies: {lost_replies}")
    print(f"Total load time (Relationships): {total_time}")
    return total_time

def read_and_load_tweets(dump_file_path):
  with open(dump_file_path) as raw_file:
    raw_text = raw_file.read()
    lines = raw_text.split('\n')
    processed_tweets = 0
    lost_mentions = 0
    unknown_geo = 0
    total_time = 0
    total_lines = len(lines)
    line_num = 0
    for line in lines:
      if len(line) > 0:
        raw_data_batch = json.loads(line)
        start = time.perf_counter()
        processed_tweets_, lost_mentions_, unknown_geo_ = load_tweets(raw_data_batch)
        processed_tweets += processed_tweets_
        lost_mentions += lost_mentions_
        unknown_geo += unknown_geo_
        end = time.perf_counter()
        batch_time = end-start
        total_time += batch_time
        line_num += 1
        print(f"Batch load time: {batch_time} - Progress {line_num / total_lines * 100}")
    print(f"Processed {processed_tweets} tweets")
    print(f"Lost mentions: {lost_mentions}")
    print(f"Unknown locations: {unknown_geo}")
    total_time += end - start
    print(f"Total load time (Tweets and Hashtags): {total_time}")
    return total_time

def read_and_load_users(dump_file_path):
  with open(dump_file_path) as raw_file:
    raw_text = raw_file.read()
    lines = raw_text.split('\n')
    processed_tweets = 0
    total_time = 0
    total_lines = len(lines)
    line_num = 0
    for line in lines:
      if len(line) > 0:
        raw_data_batch = json.loads(line)
        start = time.perf_counter()
        processed_tweets += load_users(raw_data_batch)
        load_places(raw_data_batch)
        end = time.perf_counter()
        batch_time = end-start
        total_time += batch_time
        line_num += 1
        print(f"Batch load time (Users and Places): {batch_time} - Progress {line_num / total_lines * 100}")

    print(f"Processed {processed_tweets} tweets")
    print(f"Total load time (Users and Places): {total_time}")
    return total_time

"""
Load Places dictionaries for getting geo location of tweets
"""
def load_places(raw_data):
    if ('places' in raw_data['includes']):
        for raw_place in raw_data['includes']['places']:
            if (raw_place['id'] not in departments_dict):
                departments_dict[raw_place['id']] = raw_place['name']
                countries_dict[raw_place['id']] = raw_place['country_code']

"""
Load the users into the database
"""

def load_users(raw_data):
  processed_tweets = 0
  new_users = []

  for raw_user in raw_data['includes']['users']:
    if (raw_user['id'] not in users_dict):
        user = User()
        user.id = raw_user['id']
        user.name = raw_user['name']
        user.username = raw_user['username']
        user.followers = raw_user['public_metrics']['followers_count']
        user.following = raw_user['public_metrics']['following_count']
        user.tweet_count = raw_user['public_metrics']['tweet_count']
        users_dict[user.id] = user
        new_users.append(user)
        processed_tweets += 1
  repo.save(new_users)
  return processed_tweets

total_load_time += read_and_load_users(DUMP_FILE_PATH)

"""
Load tweets.
First we load the tweets with the relationships with the author, hashtags and mentions.
Then, on a second loop, we load the relationships retweets, quotes and replies.
"""
def is_retweet(raw_tweet):
  # In case this is a retweet, return the ID of the tweet that
  # was retweeted.
  if ('referenced_tweets' not in raw_tweet):
    return None
  for ref in raw_tweet['referenced_tweets']:
    if (ref['type'] == 'retweeted'):
      return ref['id']
  return None

def load_tweets(raw_data):
    new_tweets = []
    new_hashtags = []
    processed_tweets = 0
    lost_mentions = 0
    unknown_geo = 0
    for raw_tweet in raw_data['data'] + raw_data['includes']['tweets']:
        if (raw_tweet['id'] in tweets_dict):
            continue
        else:
            # Check if we are dealing with a retweet
            id_retweeted = is_retweet(raw_tweet)
            if (id_retweeted):
              tweet = Retweet()
            else:
              tweet = Tweet()
              tweet.text = raw_tweet['text']
            tweet.id = raw_tweet['id']
            tweet.conversation_id = raw_tweet['conversation_id']
            tweet.created_at = raw_tweet['created_at']
            if ('geo' in raw_tweet):
                place_id = raw_tweet['geo']['place_id']
                if (place_id in departments_dict):
                    tweet.department = departments_dict[place_id]
                    tweet.country = countries_dict[place_id]
                else:
                    unknown_geo += 1
            public_metrics = raw_tweet['public_metrics']
            tweet.retweet_count = public_metrics['retweet_count']
            tweet.like_count = public_metrics['like_count']
            tweet.reply_count = public_metrics['reply_count']
            tweet.quote_count = public_metrics['quote_count']

            # Add the author
            user = users_dict[raw_tweet['author_id']]
            tweet.author.add(user)

            if ((id_retweeted is None) and ("entities" in raw_tweet)):
              # Add mentions to users
              if ("mentions" in raw_tweet['entities']):
                for user_id in raw_tweet['entities']['mentions']:
                  if (user_id['id'] in users_dict):
                    tweet.mentions.add(users_dict[user_id['id']])
                  else:
                    lost_mentions = lost_mentions + 1
              
              # Add hashtags
              if ("hashtags" in raw_tweet['entities']):
                for raw_hashtag in raw_tweet['entities']['hashtags']:
                  # All hashtags are converted to lowercase strings to avoid
                  # duplication
                  lowercase_hashtag = raw_hashtag['tag'].lower()
                  hashtag_match = repo.match(Hashtag, lowercase_hashtag)
                  if (lowercase_hashtag not in hashtag_dict):
                    hashtag = Hashtag()
                    hashtag.tag = lowercase_hashtag
                    hashtag_dict[lowercase_hashtag] = hashtag
                    new_hashtags.append(hashtag)
                  else:
                    hashtag = hashtag_dict[lowercase_hashtag]
                  tweet.tag.add(hashtag)

            tweets_dict[tweet.id] = tweet
            new_tweets.append(tweet)
            processed_tweets += 1

    repo.save(new_hashtags)
    repo.save(new_tweets)
    return processed_tweets, lost_mentions, unknown_geo

total_load_time += read_and_load_tweets(DUMP_FILE_PATH)

users_dict = {}
hashtag_dict = {}
countries_dict = {}
departments_dict = {}

"""
Now that tweets are loaded, we load the relationships retweet, quote and reply
"""

def load_relationships(raw_data):
  processed_tweets = 0
  lost_retweets = 0
  lost_quotes = 0
  lost_replies = 0
  updated_tweets = []
  for raw_tweet in raw_data['data'] + raw_data['includes']['tweets']:
    id_retweeted = is_retweet(raw_tweet)
    if (id_retweeted):
      if (id_retweeted not in tweets_dict):
        lost_retweets = lost_retweets + 1
      else:
        retweet = tweets_dict[raw_tweet['id']]
        retweet.rt.add(tweets_dict[id_retweeted])
        updated_tweets.append(retweet)
    else:
      if ('referenced_tweets' in raw_tweet):
        tweet = tweets_dict[raw_tweet['id']]
        for ref in raw_tweet['referenced_tweets']:
          if (ref['type'] == "quoted"):
            if (ref['id'] not in tweets_dict):
              lost_quotes = lost_quotes + 1
            else:
              tweet.quote.add(tweets_dict[ref['id']])
          elif (ref['type'] == "replied_to"):
            if (ref['id'] not in tweets_dict):
              lost_replies = lost_replies + 1
            else:
              tweet.reply.add(tweets_dict[ref['id']])
        updated_tweets.append(tweet)
    processed_tweets += 1

  repo.save(updated_tweets)
  return processed_tweets, lost_retweets, lost_quotes, lost_replies

total_load_time += read_data_and_load(DUMP_FILE_PATH, load_relationships)

print(f"TOTAL LOAD TIME: {total_load_time}")