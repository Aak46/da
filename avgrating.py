import pandas as pd
df = pd.read_csv("ratings_tiny.csv")
movieDict = {}
for index,row in df.iterrows():
	if row["movieId"] not in movieDict:
		movieDict[row["movieId"]] = []
		movieDict[row["movieId"]].append(row["rating"])
	else:
		movieDict[row["movieId"]].append(row["rating"])
for movie,ratings in movieDict.items():
	print movie,sum(ratings)/len(ratings)

