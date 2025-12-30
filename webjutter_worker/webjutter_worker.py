"""
Update what Webjutter data sources are available.
"""

from backend.lib.worker import BasicWorker

import os
import requests
import json

from json import JSONDecodeError

class WebjutterUpdater(BasicWorker):
	"""
	Update enabled Webjutter datasources and their metrics
	"""
	type = "webjutter-updater"
	max_workers = 1


	@classmethod
	def ensure_job(cls, config=None):
		"""
		Ensure that the Webjutter checker is always running

		:return:  Job parameters for the worker
		"""
		return {"remote_id": "webjutter-updater", "interval": 120}

	def work(self):
		"""
		Update Webjutter settings
		"""

		webjutter_datasources_file = self.config.PATH_ROOT / "config/extensions/webjutter_datasources.json"

		# Whether to remove the old file. This signals Webjutter is not available.
		remove_old = True

		# Only get Webjutter data if settings have been set
		if (self.config.get("webjutter-search.url") and self.config.get("webjutter-search.user") and
				self.config.get("webjutter-search.password")):

			webjutter_url = self.config.get("webjutter-search.url").strip()
			webjutter_url = webjutter_url + "/" if not webjutter_url.endswith("/") else webjutter_url
			webjutter_user = self.config.get("webjutter-search.user")
			webjutter_pw = self.config.get("webjutter-search.password")
			collections = None

			# Requests datasources overview from Webjutter API
			try:
				response = requests.get(webjutter_url + "api/overview", auth=(webjutter_user, webjutter_pw))
				if response.status_code == 200:
					collections = response.json()

			except requests.RequestException as e:
				self.log.error("Failed to update Webjutter datasources: " + str(e))

			# Store as JSON
			if collections:
				try:
					with webjutter_datasources_file.open("w") as f:
						json.dump(collections, f)
						self.log.info(f"Updated Webjutter datasources json file at {webjutter_datasources_file}")
						remove_old = False
				except JSONDecodeError:
					self.log.error("Couldn't parse Webjutter datasource.json:", collections)

		# Remove old file to indicate Webjutter is not available.
		if remove_old and os.path.isfile(webjutter_datasources_file):
			self.log.info(f"Couldn't reach the Webjutter server, removing old datasource file at {webjutter_datasources_file}")
			os.remove(webjutter_datasources_file)