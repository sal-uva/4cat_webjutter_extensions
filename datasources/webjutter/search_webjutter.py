"""
Search Webjutter collections via its API

See https://github.com/digitalmethodsinitiative/4cat-scrapers

"""

import time
import os
from pathlib import Path

from backend.lib.search import Search
from common.lib.helpers import UserInput
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException, ConfigException
from common.lib.item_mapping import MappedItem

__author__ = "Sal Hagen"
__credits__ = ["Sal Hagen"]
__maintainer__ = "Sal Hagen"
__email__ = "4cat@oilab.eu"


class SearchWebjutter(Search):
	"""
	Webjutter searcher.
	"""
	type = "webjutter-search"  # job ID. This will be changed by the Webjutter-defined data source ID later.
	category = "Search"  # category
	title = "Search Webjutter"  # title displayed in UI
	description = "Retrieve Webjutter records."  # description displayed in UI
	extension = "ndjson"  # extension of result file, used internally and in UI
	is_local = False  # Whether this datasource is locally scraped
	is_static = False  # Whether this datasource is still updated

	print("AAAAAAAAA!")

	max_workers = 1
	# For API and connection retries.
	max_retries = 3

	config = {
		# Tumblr API keys to use for data capturing
		'webjutter-search.url': {
			'type': UserInput.OPTION_TEXT,
			'default': "",
			'help': 'The URL to your Webjutter front-end server',
			'tooltip': 'If you\'re running from Docker, this defaults to <URL>:4228:80',
		},
		'webjutter-search.user': {
			'type': UserInput.OPTION_TEXT,
			'default': "",
			'help': 'Webjutter username',
			'tooltip': "This is defined in the .config file of your Webjutter server",
		},
		'webjutter-search.password': {
			'type': UserInput.OPTION_TEXT,
			'default': "",
			'help': 'Webjutter password',
			'tooltip': "This is defined in the .config file of your Webjutter server",
		},
	}
	references = ["[Webjutter documentation](https://github.com/digitalmethodsinitiative/4cat-scrapers)"]

	@classmethod
	def get_options(cls, parent_dataset=None, config=None):
		"""
		These options derive info from the `webjustter-datasources.json` file, which is collected and updated by
		`webjustter_worker.py`.
		:param config:
		"""

		# Check if webjutter_datasources.json exists in the same directory as this file
		datasources_file = config.PATH_ROOT / "webjutter_datasources.json"
		print(datasources_file)
		if not datasources_file.exists():
			return {
				"intro": {
					"type": UserInput.OPTION_INFO,
					"help": "Webjutter is not configured. Please configure it in the Webjutter settings."
				}
			}
		else:
			return {
			"intro": {
				"type": UserInput.OPTION_INFO,
				"help": "Retrieve any kind of Webjutter item."
			}
		}

	def get_items(self, query):
		"""
		Fetches data from Webjutter via its ES and Mongo-enabled API.

		"""

		# ready our parameters
		parameters = self.dataset.get_parameters()
		results = []

		self.job.finish()
		return results


	@staticmethod
	def map_item(item):
		"""
		Parse Webjutter items. Most of these can just be mapped directly.
		:param item:		Item as returned by the Webjutter API.

		:return dict:		Mapped item
		"""

		return MappedItem({item})
