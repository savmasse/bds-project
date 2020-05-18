import pandas as pd
from pandas import Series
import matplotlib.pyplot
import re

from PreProcessTweets import PreProcessTweets


class Visualisation:

	def __init__(self, data):
		names = ["text", "tags", "user", "time", "location", "coordinates"]

		if isinstance(data, 'str'.__class__):
			self.dataframe = pd.read_csv(data, delimiter=';', names=names, header=None)
		else:
			self.dataframe = data

	def trending_tags_global(self):
		df_no_nan = pd.DataFrame({'tags': self.dataframe.tags.dropna()})
		tags_df = pd.concat([Series(row['tags'].split('|')) for _, row in df_no_nan.iterrows()]).reset_index()
		tags_df.columns = ['?', 'tags']
		tags_df = tags_df.drop('?', axis=1)
		tags_df.tags = tags_df.tags.str.lower()
		tags_count = tags_df['tags'].value_counts().rename_axis('tag').reset_index(name='counts')
		return tags_count.nlargest(10, 'counts').plot.bar(x='tag', y='counts')

	def trending_tags_local(self, city, country):
		pass

	def get_dataframe(self):
		return self.dataframe
