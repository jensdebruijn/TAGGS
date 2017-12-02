import math
import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import pairwise_distances

my_token_pattern = r"\w+(?:-\w+)+|[-+]?\d+[.,]?\d+|[#@]?\w+\b|[\U00010000-\U0010ffff\U0001F300-\U0001F64F\U0001F680-\U0001F6FF\u2600-\u26FF\u2700-\u27BF]|[.:()[],;?!*]{2,4}"


def eliminate_near_duplicate_tweets(tweetsDF, distancemetric='cosine', debug=False, similarity_threshold=0.20, debug_threshold=1000, defaultfreqcut_off=2, n_jobs=1):
    start_time = datetime.datetime.now()
    if len(tweetsDF) > 1000:
        freqcutoff = int(math.log(len(tweetsDF)))
    else:
        freqcutoff = defaultfreqcut_off  # default 2 is applicable for short texts tweets.

    if len(tweetsDF) > 1000:
        word_vectorizer = TfidfVectorizer(ngram_range=(1, 2), lowercase=False, norm='l2', min_df=freqcutoff, token_pattern=my_token_pattern, sublinear_tf=True)
    else:  # otherwise min_df is bigger than max_df ValueError:
        word_vectorizer = TfidfVectorizer(ngram_range=(1, 2), lowercase=False, norm='l2', token_pattern=my_token_pattern, sublinear_tf=True)

    X2_train = word_vectorizer.fit_transform(tweetsDF['text'])

    allowed_metrics = ['cosine',  'euclidean', 'cityblock', 'jaccard']
    if distancemetric not in allowed_metrics:
        raise Exception("distance metric should be one of the allowed ones. Allowed metrics are: " + str(allowed_metrics))

    dist_matrix = pairwise_distances(X2_train, metric=distancemetric, n_jobs=n_jobs)   # Valid values for metric are 'Cosine', 'Cityblock', 'Euclidean' and 'Manhattan'.

    similarity_dict = {}
    for a, b in np.column_stack(np.where(dist_matrix < similarity_threshold)):  # zip(np.where(overthreshold)[0],np.where(overthreshold)[1]):
        if a != b:
            if tweetsDF.index[a] not in similarity_dict:  # work with the actual index no in the dataframe, not with the order based one!
                similarity_dict[tweetsDF.index[a]] = [tweetsDF.index[a]]  # a is the first member of the group.
            similarity_dict[tweetsDF.index[a]].append(tweetsDF.index[b])

    if len(similarity_dict) == 0:
        return tweetsDF

    cluster_tuples_list = list(set([tuple(sorted(km)) for km in similarity_dict.values()]))  # for each element have a group copy in the group, decrease 1.
    cluster_tuples_list = sorted(cluster_tuples_list, key=len, reverse=True)

    cluster_tuples2 = [cluster_tuples_list[0]]

    duplicate_tweet_indexes = list(cluster_tuples_list[0])
    for ct in cluster_tuples_list[1:]:
        if len(set(duplicate_tweet_indexes) & set(ct)) == 0:
            cluster_tuples2.append(ct)
            duplicate_tweet_indexes += list(ct)

    duplicate_tweet_indexes = list(set(duplicate_tweet_indexes))

    one_index_per_duplicate_group = []
    # If multiple tweets are similar, pick the oldest one and therefore, most likely the original one
    if 'date' in tweetsDF.columns:
        for clst in cluster_tuples2:
            last_date = tweetsDF.ix[[i for i in clst]].sort_values(by='date').iloc[0]
            one_index_per_duplicate_group.append(last_date._name)
    else:
        for clst in cluster_tuples2:
            one_index_per_duplicate_group.append(clst[0])

    indexes_of_the_uniques = [i for i in tweetsDF.index if i not in duplicate_tweet_indexes]

    unique_tweetsDF = tweetsDF.ix[[i for i in tweetsDF.index if i not in duplicate_tweet_indexes]+one_index_per_duplicate_group]  # +

    tweet_sets = []
    for i, ct in enumerate(cluster_tuples2):
        tweets = []
        for t_indx in ct:
            tweets.append(tweetsDF['text'].ix[t_indx])
        tweet_sets.append(tweets)

    return unique_tweetsDF
