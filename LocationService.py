import certifi
import ssl
import geopy
from opencage.geocoder import OpenCageGeocode
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter


class LocationService:

	def __init__(self):
		ctx = ssl.create_default_context(cafile=certifi.where())
		geopy.geocoders.options.default_ssl_context = ctx
		self.locator = geopy.Nominatim(user_agent="BDS_project")
		self.geocoder = OpenCageGeocode("f3d708b60c224f75959258777933a2ec")

	def get_coordinates(self, city, country):
		query = city + ", " + country
		location = self.locator.geocode(query)
		if location is not None:
			lat = location.latitude
			lng = location.longitude
			return lat, lng
		else:
			return None

	def add_location_data(self, file):
		names = ["text", "tags", "user", "time", "location", "coordinates"]
		if isinstance(file, 'str'.__class__):
			df = pd.read_csv(file, delimiter=';', names=names, header=None)
		elif isinstance(file, pd.DataFrame.__class__):
			df = file
		else:
			return "argument should be csv filepath or panda dataframe"
		df = df.dropna(subset=['location'])
		print(df.count())
		# 1 - conveneint function to delay between geocoding calls
		geocode = RateLimiter(self.locator.geocode, min_delay_seconds=0.5)
		# 2- - create location column
		df['processed_location'] = df['location'].apply(geocode)
		# 3 - create longitude, laatitude and altitude from location column (returns tuple)
		df['point'] = df['processed_location'].apply(lambda loc: tuple(loc.point) if loc else None)
		# 4 - split point column into latitude, longitude and altitude columns
		#df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=df.index)
		return df
