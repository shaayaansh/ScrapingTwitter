import snscrape.modules.twitter as sntwitter
import pandas as pd
import os


data_path = "PycharmProjects/FNN/real.csv"   # The dataset csv file
conversations_path = "PycharmProjects/FNN/Data/Real"    # Where I store the conversation folders
data = pd.read_csv(data_path)


for i, row in data.iterrows():
    article_path = os.path.join(conversations_path, f'Article_{i}') # New Line
"""     if i < 100:
        continue
    print(type(row['tweet_ids']), row['tweet_ids'])
    if i > 130:
        break """
    if isinstance(row['tweet_ids'], float):
        print(row['tweet_ids'])
        continue
    for id in row['tweet_ids'].split():
        print(id)
        directory_name = str(id)
        folder_path = os.path.join(article_path, directory_name)
        tweet_generator = sntwitter.TwitterTweetScraper(id).get_items() # Getting the tweet based on its ID
        try:
            for tweet in tweet_generator:
                if not isinstance(tweet, sntwitter.Tombstone): # Checking whether the tweet is deleted or not
                    tweet_df = {'id': tweet.id, 'rawContent': tweet.rawContent,
                        'conversationId': tweet.conversationId, 'source': tweet.source, 'reply_count': tweet.replyCount, 
                        'like_count': tweet.likeCount, 'user': tweet.user, 'quote_count': tweet.quoteCount, 'media': tweet.media,
                        'hashtags': tweet.hashtags, 'in_reply_to_tweet_id': tweet.inReplyToTweetId}
                    tweet_df = pd.DataFrame.from_records([tweet_df])    
                        

                    if tweet.replyCount > 0: # Checking if the tweet has a thread
                        os.makedirs(folder_path, exist_ok=True)
                        tweet_df.to_csv(os.path.join(folder_path, f'{id}_tweet.csv'))
                        query = f'conversation_id:{id} filter:safe'
                        conversation = sntwitter.TwitterSearchScraper(query).get_items()
                        df = pd.DataFrame(columns=['id', 'rawContent', 'conversation_id', 'source', 'reply_count', 'like_count',
                                                    'user', 'quote_count', 'media', 'hashtags', 'in_reply_to_tweet_id'])
                        for tweet in conversation:
                            df2 = {'tweet_id': str(tweet.id), 'rawContent': tweet.rawContent, 
                            'conversation_id': str(tweet.conversationId), 'source': tweet.source, 'reply_count': tweet.replyCount, 
                            'like_count': tweet.likeCount, 'user': tweet.user, 'quote_count': tweet.quoteCount, 'media': tweet.media,
                            'hashtags': tweet.hashtags, 'in_reply_to_tweet_id': tweet.inReplyToTweetId}
                            df = pd.concat([df, pd.DataFrame.from_records([df2])])
                        df.to_csv(os.path.join(folder_path, f'{id}.csv'))
        except KeyError:
            pass