import pandas as pd
from pandas import Series
import matplotlib.pyplot
from LocationService import LocationService
import folium
from folium.plugins import HeatMap

from PreProcessTweets import PreProcessTweets


class Visualisation:

	def __init__(self, data):
		names = ["tweet_id", "text", "location", "user", "time", "label", "tags"]
		if isinstance(data, 'str'.__class__):
			self.dataframe = pd.read_csv(data, delimiter=';', names=names, header=None)
		else:
			self.dataframe = data

	def trending_tags_global(self, data=None):
		if data is None:
			df = self.dataframe.drop(self.dataframe[self.dataframe.label != 0].index)
		else:
			df = data.drop(data[data.label != 0].index)
		df_no_nan = pd.DataFrame({'tags': df.tags.dropna()})
		tags_df = pd.concat([Series(row['tags'].split('|')) for _, row in df_no_nan.iterrows()]).reset_index()
		tags_df.columns = ['?', 'tags']
		tags_df = tags_df.drop('?', axis=1)
		tags_df.tags = tags_df.tags.str.lower()
		tags_count = tags_df['tags'].value_counts().rename_axis('tag').reset_index(name='counts')
		return tags_count.nlargest(10, 'counts').plot.bar(x='tag', y='counts')

	def trending_tags_local(self, city, country, radius):
		if 'coordinates' not in self.dataframe:
			print("No coordinates in dataframe, add them first with LocationService")
		loc = LocationService()
		coordinates = loc.get_coordinates(city, country)
		loc.set_point(coordinates[0], coordinates[1])
		loc.set_range(radius)
		df_location = self.dataframe.drop(self.dataframe[self.dataframe.coordinates.apply(lambda x: loc.in_range(x) is not True)].index)
		return self.trending_tags_global(df_location)

	def heat_map(self):
		if 'coordinates' not in self.dataframe:
			print("No coordinates in dataframe, add them first with LocationService")
		heat_map = folium.Map(location=[0, 0], zoom_start=2)
		points = self.dataframe.dropna().coordinates.tolist()
		HeatMap(points).add_to(heat_map)
		return heat_map

	def get_dataframe(self):
		return self.dataframe
