import pandas as pd
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
df = pd.read_csv("pima-indians-diabetes.csv")
train,test = train_test_split(df,test_size=0.2)
X = train.loc[:,"Pregnancies":"DiabetesPedigreeFunction"]
y = train["Class"]
X_test = test.loc[:,"Pregnancies":"DiabetesPedigreeFunction"]
y_test = test["Class"]
classifier = GaussianNB()   # perform online updates to model parameters
#training
classifier.fit(X,y)
y_predicted = classifier.predict(X_test)
score = accuracy_score(y_test,y_predicted)
print "Accuracy ",score





