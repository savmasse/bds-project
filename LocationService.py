import certifi
import ssl
import geopy.distance
from opencage.geocoder import OpenCageGeocode
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter


class LocationService:

	def __init__(self):
		self.point = [0, 0]
		self.range = 100
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
		else:
			df = file
		df = df.dropna(subset=['location'])
		print(df.count())
		geocode = RateLimiter(self.locator.geocode, min_delay_seconds=0.5)
		df['coordinates'] = df['location'].apply(geocode).apply(lambda loc: [loc.point.latitude, loc.point.longitude] if loc else None)
		return df

	def set_point(self, latitude, longitude):
		self.point = [latitude, longitude]

	def set_range(self, range):
		self.range = range

	def in_range(self, point):
		if geopy.distance.vincenty(self.point, point).km <= self.range:
			return True
		else:
			return False
