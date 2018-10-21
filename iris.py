import pandas as pd 
import matplotlib.pyplot as plt
df = pd.read_csv("iris.data")
print df.head()
print df.info()
print df.describe()
print "***************"
print df["sepal_length"]
print df[["sepal_length","petal_length"]]

print df.columns.values
for col in df.columns.values[:-1]:
	print col
	df.hist(column=col)
plt.show()
df.boxplot(column=list(df.columns.values[:-1]))
plt.show()
