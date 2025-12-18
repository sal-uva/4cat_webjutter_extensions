"""
Search Webjutter collections via its API

See https://github.com/digitalmethodsinitiative/4cat-scrapers

"""

import requests
import json
import time
import pandas as pd

from backend.lib.search import Search
from common.lib.helpers import UserInput, strip_tags
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException
from common.lib.item_mapping import MappedItem

from requests.exceptions import ConnectionError

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
	description = "Retrieve Webjutter data."  # description displayed in UI
	extension = "ndjson"  # extension of result file, used internally and in UI
	is_local = False  # Whether this datasource is locally scraped
	is_static = False  # Whether this datasource is still updated

	max_workers = 1
	# For API and connection retries.
	max_retries = 3

	datasources = {}

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
			'sensitive': True
		},
	}
	references = ["[Webjutter documentation](https://github.com/digitalmethodsinitiative/4cat-scrapers)"]

	@classmethod
	def get_options(cls, parent_dataset=None, config=None):
		"""
		These options derive info from the `webjutter-datasources.json` file, which is collected and updated by
		`webjustter_worker.py`.
		:param config:
		"""

		# Check if webjutter_datasources.json exists in the same directory as this file
		datasources_file = config.PATH_ROOT / "config/extensions/webjutter_datasources.json"
		if not config.get("webjutter-search.url") or not config.get("webjutter-search.user") or not config.get("webjutter-search.password"):
			return {
				"error": {
					"type": UserInput.OPTION_INFO,
					"help": "<code>Webjutter is not configured. Insert a valid URL and login in the Control Panel or "
							"ask the admin to do so.</code>"
				}
			}

		elif not datasources_file.exists():
			return {
				"error": {
					"type": UserInput.OPTION_INFO,
					"help": "<code>Webjutter is configured but could not reach it. Make sure the Webjutter Search "
							"settings are valid in the Control Panel.</code>"
				}
			}

		# We have a datasource json from Webjutter to work with, use this for input fields
		try:
			cls.datasources = json.load(open(datasources_file, "r"))
		except json.JSONDecodeError:
			return {
				"error": {
					"type": UserInput.OPTION_INFO,
					"help": "<code>Webjutter is configured and reachable, but the available datasources couldn't be "
							"read.</code>"
				}
			}

		def create_metadata_table(data, header="", ds_data=None):
			"""Create combined metadata table with records info and metadata"""

			# Add total records to metadata table
			table_rows = []
			if header == "Metadata" and ds_data:
				table_rows = [
					["Total records", ds_data.get("elastic", {}).get("total_records", "unknown")],
					["Latest", ds_data.get("elastic", {}).get("up_until", "unknown")]
				]

			# Flatten nested metadata structure
			def flatten_metadata(data_dict, prefix=""):
				if not isinstance(data_dict, dict):
					return
				for key, value in data_dict.items():
					if isinstance(value, dict):
						# If it's a nested dict, recurse with updated prefix
						if prefix:
							new_prefix = f"{prefix}_{key}"
						else:
							new_prefix = key
						flatten_metadata(value, new_prefix)
					else:
						label = f"{prefix}_{key}" if prefix else key
						table_rows.append([label, value])

			# Only flatten if data exists
			if data:
				flatten_metadata(data)

			if not table_rows:
				return "No data available"

			combined_df = pd.DataFrame(table_rows)
			html_table = combined_df.to_html(header=False, index=False)
			if header:
				header_row = f'<tr><th colspan="2" style="text-align: center; font-weight: bold;">{header}</th></tr>'
				html_table = html_table.replace('<tbody>', f'<tbody>{header_row}')
			return html_table

		# Get option data from api/overview json file
		datasource_labels = {ds_id: ds_values.get("name", ds_id)
							 for ds_id, ds_values in cls.datasources["collections"].items()}

		return {
			"intro": {
				"type": UserInput.OPTION_INFO,
				"help": "Retrieve any kind of Webjutter item."
			},
			"webjutter_datasource": {
				"type": UserInput.OPTION_CHOICE,
				"help": "Webjutter collection",
				"options": {
					**datasource_labels
				}
			},
			# Dynamic info fields for each datasource
			# Description
			**{
				f"{ds_id}_description": {
					"type": UserInput.OPTION_INFO,
					"help": ds_data.get("description"),
					"requires": f"webjutter_datasource=={ds_id}",
				} for ds_id, ds_data in cls.datasources["collections"].items() if ds_data.get("description")
			},
			# For metadata field:
			**{
				f"{ds_id}_metadata": {
					"type": UserInput.OPTION_INFO,
					"help": create_metadata_table(ds_data.get("metadata"), header="Metadata", ds_data=ds_data),
					"requires": f"webjutter_datasource=={ds_id}",
				} for ds_id, ds_data in cls.datasources["collections"].items() if ds_data.get("metadata")
			},
			# For query fields:
			**{
				f"{ds_id}_query_fields": {
					"type": UserInput.OPTION_INFO,
					"help": create_metadata_table(ds_data.get("fields"), header="Search fields"),
					"requires": f"webjutter_datasource=={ds_id}",
				} for ds_id, ds_data in cls.datasources["collections"].items()
				if ds_data.get("fields") and "Search fields" not in ds_data.get("description", "")
			},
			# Indexed fields
			# Query field
			"query_header": {
				"type": UserInput.OPTION_INFO,
				"help": "### Querying",
			},
			"query_info": {
				"type": UserInput.OPTION_INFO,
				"help": "Webjutter uses [Elasticsearch's query string syntax](https://www.elastic.co/docs/reference/query-languages/query-dsl/"
						"query-dsl-query-string-query#query-string-syntax). Make sure to use the correct field names "
						"and operators.<br><strong>Example 1:</strong> <code>author:\"John Smith\" OR body:qu?ck bro*</code><br><strong>"
						"Example 2:</strong> <code>hashtag:(liminal space) AND timestamp:[2012-01-01 TO 2012-12-31]</code>",
			},
			**{
				f"{ds_id}_query_fields": {
					"type": UserInput.OPTION_INFO,
					"help": create_metadata_table(ds_data["fields"], header="Search fields"),
					"requires": f"webjutter_datasource=={ds_id}",
				} for ds_id, ds_data in cls.datasources["collections"].items()
				if "fields" in ds_data and "Search fields" not in ds_data.get("description", "")
			},
			"query": {
				"type": UserInput.OPTION_TEXT_LARGE,
				"help": "Query",
				"tooltip": "See the ElasticSearch documentation for instructions",
			}
		}


	def get_items(self, query):
		"""
		Fetches data from Webjutter via its ES and Mongo-enabled API.

		"""

		# ready our parameters
		self.dataset.update_status("Preparing parameters")
		parameters = self.dataset.get_parameters()

		webjutter_url = self.config.get("webjutter-search.url")
		user = self.config.get("webjutter-search.user")
		password = self.config.get("webjutter-search.password")
		datasource = parameters.get("webjutter_datasource")
		search_query = parameters.get("query")

		results = []
		total_records = "unknown"
		retries = 0
		has_more = True
		search_after = None  # For ElasticSearch pagination. Used instead of tokens because of large datasets.

		# Build base URL
		base_url = f"{webjutter_url}/api/{datasource}/search"

		self.dataset.update_status(f"Connecting to Webjutter at {base_url}")

		while has_more and retries <= self.max_retries:
			if self.interrupted:
				raise ProcessorInterruptedException(f"Interrupted while fetching items from {datasource} via Webjutter")

			# Build URL with parameters
			params = {"q": search_query}
			if search_after:
				params["search_after"] = search_after

			# Get the data
			try:
				response = requests.get(base_url, params=params, auth=(user, password))
				response.raise_for_status()
			except ConnectionError as e:
				self.log.error(f"Failed to fetch items from {datasource} via Webjutter, waiting 2 seconds: {e}")
				time.sleep(2)
				retries += 1
				continue
			except requests.exceptions.HTTPError as e:
				if response.status_code == 429:  # Too Many Requests
					wait_time = min(2 ** retries, 60)  # Exponential backoff, max 60 seconds
					self.dataset.update_status(f"Rate limited, waiting {wait_time}s before retry")
					time.sleep(wait_time)
					retries += 1
					continue
				else:
					raise e
			except Exception as e:
				self.log.error(f"Failed to fetch items from {datasource} via Webjutter: {e}")
				return results

			# Got the data, now parse it
			try:
				response_data = response.json()
			except json.JSONDecodeError as e:
				self.log.error(f"Invalid JSON response from Webjutter: {e}")
				return results

			items = response_data.get("results", [])
			if not items:
				break

			total_records = response_data.get("total", total_records)

			results.extend(items)
			retries = 0

			# Check for search_after pagination
			search_after = response_data.get("search_after")
			if search_after:
				self.dataset.update_status(f"Retrieved {len(results)}/{total_records} items")
				if isinstance(total_records, int) and total_records > 0:
					self.dataset.update_progress(len(results) / total_records)
				has_more = True
				time.sleep(.5)
			else:
				has_more = False

		self.job.finish()
		return results

	@staticmethod
	def map_item(item):
		"""
		Parse Webjutter items. Most of these can just be mapped directly.
		:param item:		Item as returned by the Webjutter API.

		:return dict:		Mapped item
		"""
		# Ensure we have an 'id', 'author', and 'body' column, required for 4CAT
		# 4chan / 8kun
		if item.get("board") and "no" in item:
			# todo: make this dynamic, but there's a lot of differences between schemas
			KNOWN_CHAN_FIELDS = ("no","resto","board","sub","com","time","now","name","deleted","timestamp_deleted","replies_to","id","capcode","trip","filename","tim","md5","w","h","tw","th","fsize","country","country_name","op","replies","images","semantic_url","sticky","closed","archived_on","scraped_on","modified_on","unique_ips","bumplimit","imagelimit")
			REPLACED_FIELDS = ("no", "id", "resto", "com", "board", "time", "now", "name", "sub", "com")
			item = {
				"board": item.get("board", ""),
				"id": item.get("no", ""),
				"id_in_thread": item.get("id", "")
				"thread_id": item.get("no") if not item.get("resto") else item.get("resto"),
				"unix_timestamp": item.get("time", ""),
				"etd_timestamp": item.get("now", ""),
				"author": item.get("name", ""),
				"title": strip_tags(item.get("sub", "")),
				"body": strip_tags(item.get("com", "")),
				**{field: item.get(field, "") for field in KNOWN_CHAN_FIELDS if field not in REPLACED_FIELDS}
			}

		return MappedItem(item)

	@staticmethod
	def validate_query(query, request, config):
		""" Validate Webjutter query. Todo: Use ElasticSearch's query validation"""

		# no query 4 u
		if not query.get("query", "").strip():
			raise QueryParametersException("You must provide a search query.")

		if not query.get("webjutter_datasource", "").strip():
			raise QueryParametersException("You must provide a Webjutter datasource.")

		return query


	def after_process(self):
		"""
		Change the datasource type to the one used in the query.

		"""
		self.dataset.change_datasource(self.parameters.get("webjutter_datasource", "webjutter"))
		super().after_process()