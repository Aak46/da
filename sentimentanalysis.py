import re
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.metrics import accuracy_score
import warnings
warnings.filterwarnings("ignore")
def process_tweet(tweet):
    return " ".join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])", "",tweet.lower()).split())
def drop_features(features,data):
    data.drop(features,inplace=True,axis=1)

train_data = pd.read_csv("train_tweets.csv")
train_data['processed_tweets'] = train_data['tweet'].apply(process_tweet)
drop_features(['id','tweet'],train_data)

x_train, x_test, y_train, y_test = train_test_split(train_data["processed_tweets"],train_data["label"], test_size = 0.2, random_state = 42)
count_vect = CountVectorizer(stop_words='english')
transformer = TfidfTransformer(norm='l2',sublinear_tf=True)

x_train_counts = count_vect.fit_transform(x_train)
x_train_tfidf = transformer.fit_transform(x_train_counts)
x_test_counts = count_vect.transform(x_test)
x_test_tfidf = transformer.transform(x_test_counts)

from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier(n_estimators=200)
model.fit(x_train_tfidf,y_train)

predictions = model.predict(x_test_tfidf)
print accuracy_score(y_test,predictions)

test_data = pd.read_csv('test_tweets.csv')
test_data['processed_tweet'] = test_data['tweet'].apply(process_tweet)
drop_features(['tweet'],test_data)

train_counts = count_vect.fit_transform(train_data['processed_tweets'])
test_counts = count_vect.transform(test_data['processed_tweet'])
train_tfidf = transformer.fit_transform(train_counts)
test_tfidf = transformer.transform(test_counts)
model.fit(train_tfidf,train_data['label'])

predictions = model.predict(test_tfidf)
final_result = pd.DataFrame({'id':test_data['id'],'label':predictions})
final_result.to_csv('output.csv',index=False)

