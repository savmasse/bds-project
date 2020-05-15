from opencage.geocoder import OpenCageGeocode


class LocationService:

	def __init__(self):
		self.key = "f3d708b60c224f75959258777933a2ec"
		self.geocoder = OpenCageGeocode(self.key)

	def get_coordinates(self, city, country):
		query = city + ", " + country
		location_details = self.geocoder.geocode(query)
		lat = location_details[0]['geometry']['lat']
		lng = location_details[0]['geometry']['lng']
		return lat, lng