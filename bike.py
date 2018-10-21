import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
df = pd.read_csv("2010-capitalbikeshare-tripdata.csv")
#print df["Member type"].unique()
df.loc[df["Member type"] == 'Member', "Member type"] = 1
df.loc[df["Member type"] == 'Casual', "Member type"] = 2
df.loc[df["Member type"] == 'Unknown', "Member type"] = 3

print df["Member type"]
train,test = train_test_split(df,test_size=0.3)
X = train[["Duration","Start station number","End station number"]]
y = train["Member type"].astype(int)
X_test = test[["Duration","Start station number","End station number"]]
y_test = test["Member type"].astype(int)
classifier = LogisticRegression(multi_class="ovr")
classifier.fit(X,y)
y_predicted = classifier.predict(X_test)
score = accuracy_score(y_test,y_predicted)
print "Accuracy ",score



