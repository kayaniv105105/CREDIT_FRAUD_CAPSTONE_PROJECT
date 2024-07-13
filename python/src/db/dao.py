import happybase

class HBaseDao:
	"""
	Dao class for operation on HBase
	"""
	__instance = None

	@staticmethod
	def get_instance():
		""" Static access method. """
		if HBaseDao.__instance == None:
			HBaseDao()
		return HBaseDao.__instance

	def __init__(self):
		if HBaseDao.__instance != None:
			raise Exception("This class is a singleton!")
		else:
			HBaseDao.__instance = self
			self.host = '44.214.91.136'
			self.connect()
			

	def connect(self):
		for i in range(3):
			try:
				self.pool = happybase.ConnectionPool(size=3, host=self.host, port=9090)
				break
			except:
				print("Exception in connecting HBase")



	def get_data(self, key, table):
		for i in range(2):
			try:
				with self.pool.connection() as connection:
					t = connection.table(table)
					row = t.row(key)
					return row
			except:
				self.reconnect()



	def write_data(self, key, row, table):
		error = None
		for i in range(2):
			try:
				with self.pool.connection() as connection:
					t = connection.table(table)
					return t.put(key, row)
			except Exception as e:
				error = e
				self.reconnect()
		raise Exception(key + str(e))

	def reconnect(self):
		self.connect()
