"""
Search Webjutter collections via its API

See https://github.com/digitalmethodsinitiative/4cat-scrapers

"""

import requests
import json
import time
import pandas as pd
from requests import JSONDecodeError
from requests.exceptions import ConnectionError, Timeout, RequestException, HTTPError
from backend.lib.search import Search
from common.lib.helpers import UserInput, strip_tags
from common.lib.exceptions import (
    QueryParametersException,
    ProcessorInterruptedException,
    ConfigException,
    QueryNeedsExplicitConfirmationException,
)
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
    title = "Webjutter search"  # title displayed in UI
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
        "webjutter-search.url": {
            "type": UserInput.OPTION_TEXT,
            "default": "",
            "help": "The URL to your Webjutter front-end server",
            "tooltip": "If you're running from Docker, this defaults to <URL>:4228:80",
        },
        "webjutter-search.user": {
            "type": UserInput.OPTION_TEXT,
            "default": "",
            "help": "Webjutter username",
            "tooltip": "This is defined in the .config file of your Webjutter server",
        },
        "webjutter-search.password": {
            "type": UserInput.OPTION_TEXT,
            "default": "",
            "help": "Webjutter password",
            "tooltip": "This is defined in the .config file of your Webjutter server",
            "sensitive": True,
        },
    }
    references = [
        "[Webjutter documentation](https://github.com/digitalmethodsinitiative/4cat-scrapers)"
    ]

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        """
        These options derive info from the `webjutter-datasources.json` file, which is collected and updated by
        `webjustter_worker.py`.
        :param config:
        """

        # Check if webjutter_datasources.json exists in the same directory as this file
        datasources_file = (
                config.PATH_ROOT / "config/extensions/webjutter_datasources.json"
        )
        if (
                not config.get("webjutter-search.url")
                or not config.get("webjutter-search.user")
                or not config.get("webjutter-search.password")
        ):
            return {
                "error": {
                    "type": UserInput.OPTION_INFO,
                    "help": "<code>Webjutter is not configured. Insert a valid URL and login in the Control Panel or "
                            "ask the admin to do so.</code>",
                }
            }

        elif not datasources_file.exists():
            return {
                "error": {
                    "type": UserInput.OPTION_INFO,
                    "help": "<code>Webjutter is configured but could not reach it. Make sure the Webjutter Search "
                            "settings are valid in the Control Panel.</code>",
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
                            "read.</code>",
                }
            }

        def create_metadata_table(data, header=""):
            """Create combined metadata table with records info and metadata"""

            # Add total records to metadata table
            table_rows = []

            # Flatten nested metadata structure
            def flatten_metadata(data_dict, prefix=""):
                if not isinstance(data_dict, dict):
                    return
                for key, value in data_dict.items():
                    if isinstance(value, dict):
                        # If it's a nested dict, recurse with updated prefix
                        if prefix:
                            new_prefix = f"{prefix} {key}"
                        else:
                            new_prefix = key
                        flatten_metadata(value, new_prefix)
                    else:
                        label = f"{prefix} {key}" if prefix else key
                        table_rows.append([label, value])

            # Only flatten if data exists
            if data:
                flatten_metadata(data)

            if not table_rows:
                return "No data available"

            combined_df = pd.DataFrame(table_rows)
            # Format numeric columns
            for col in combined_df.columns:
                if combined_df[col].dtype in ['int64', 'float64']:
                    combined_df[col] = combined_df[col].apply(lambda x: f"{x:,}")

            html_table = combined_df.to_html(header=False, index=False)
            if header:
                header_row = f'<tr><th colspan="2" style="text-align: center; font-weight: bold;">{header}</th></tr>'
                html_table = html_table.replace("<tbody>", f"<tbody>{header_row}")
            return html_table

        # Get option data from api/overview json file
        datasource_labels = {
            ds_id: ds_values.get("name", ds_id)
            for ds_id, ds_values in cls.datasources["collections"].items()
        }

        return {
            "intro": {
                "type": UserInput.OPTION_INFO,
                "help": "Retrieve any kind of Webjutter item. Max 10 million items are returned.",
            },
            "webjutter_datasource": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Webjutter collection",
                "options": {**datasource_labels},
            },
            # Dynamic info fields for each datasource
            # Description
            **{
                f"{ds_id}_description": {
                    "type": UserInput.OPTION_INFO,
                    "help": ds_data.get("description"),
                    "requires": f"webjutter_datasource=={ds_id}",
                }
                for ds_id, ds_data in cls.datasources["collections"].items()
                if ds_data.get("description")
            },
            # For metadata field:
            **{
                f"{ds_id}_metadata": {
                    "type": UserInput.OPTION_INFO,
                    "help": create_metadata_table(
                        ds_data.get("metadata"), header="Metadata"
                    ),
                    "requires": f"webjutter_datasource=={ds_id}",
                }
                for ds_id, ds_data in cls.datasources["collections"].items()
                if ds_data.get("metadata")
            },
            # Query field
            "query_header": {
                "type": UserInput.OPTION_INFO,
                "help": "### Querying",
            },
            "query_info": {
                "type": UserInput.OPTION_INFO,
                "help": "Webjutter uses [Elasticsearch's query string syntax](https://www.elastic.co/docs/reference/query-languages/query-dsl/"
                "query-dsl-query-string-query#query-string-syntax). Make sure to use the correct field names "
                'and operators.<br><strong>Example 1:</strong> <code>author:"John Smith" OR body:qu?ck bro*</code><br><strong>'
                "Example 2:</strong> <code>hashtag:(liminal space) AND timestamp:[2012-01-01 TO 2012-12-31]</code>",
            },
            "query_info_fourchan": {
                "type": UserInput.OPTION_INFO,
                "help": "<strong>Example 4chan query:</strong> <code>board:mu AND com:*saxophone* AND time:[2020-01-01 "
                "TO 2022-12-31]</code>",
                "requires": "webjutter_datasource==fourchan",
            },
            # For query fields:
            **{
                f"{ds_id}_query_fields": {
                    "type": UserInput.OPTION_INFO,
                    "help": create_metadata_table(
                        ds_data.get("search_fields"), header="Search fields"
                    ),
                    "requires": f"webjutter_datasource=={ds_id}",
                }
                for ds_id, ds_data in cls.datasources["collections"].items()
                if ds_data.get("search_fields")
            },
            "board": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Board",
                "options": {board_name: board_name for board_name in
                            cls.datasources.get("collections", {}).get("fourchan", {}).get("metadata", {})
                            .get("board", {}).keys()},
                "requires": "webjutter_datasource==fourchan",
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "Query",
                "tooltip": "See the ElasticSearch documentation for instructions",
            },
        }

    def get_items(self, query):
        """
        Fetches data from Webjutter via its ES and Mongo-enabled API.

        """

        # ready our parameters
        self.dataset.update_status("Preparing parameters")
        parameters = self.dataset.get_parameters()

        url = self.config.get("webjutter-search.url", "")
        user = self.config.get("webjutter-search.user", "")
        password = self.config.get("webjutter-search.password", "")
        datasource = parameters.get("webjutter_datasource")
        search_query = parameters.get("query")

        # Board
        board = parameters.get("board")
        if not board:
            self.dataset.update_status("No board selected")
            self.dataset.finish(-1)
            return

        search_query = f"board:{board} AND {search_query}"

        results = []
        total_records = 0
        retries = 0
        has_more = True
        search_after = None  # For ElasticSearch pagination. Used instead of tokens because of large datasets.

        self.dataset.update_status(f"Connecting to Webjutter")

        while has_more and retries <= self.max_retries:
            if self.interrupted:
                raise ProcessorInterruptedException(
                    f"Interrupted while fetching items from {datasource} via Webjutter"
                )

            # Build URL with parameters
            params = {"q": search_query}
            if search_after:
                params["search_after"] = search_after

            # Send the request
            try:
                request_results = self.webjutter_search_request(params, datasource, url, user, password)
            except (ConnectionError, Timeout, RequestException, HTTPError, JSONDecodeError) as e:
                self.dataset.update_status("Error reaching webjutter", str(e))
                self.dataset.finish(-1)
                return

            items = request_results["results"]
            if not items:
                break

            results.extend(items)
            total_records = request_results.get("total", total_records)

            # Check for search_after pagination
            search_after = request_results.get("search_after")
            if search_after:
                self.dataset.update_status(
                    f"Retrieved {len(results):,}/{total_records:,} items"
                )
                if total_records > 0:
                    self.dataset.update_progress(len(results) / total_records)
                has_more = True
                time.sleep(0.5)
            else:
                has_more = False

            retries = 0

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
            KNOWN_CHAN_FIELDS = (
                "now",
                "deleted",
                "timestamp_deleted",
                "replies_to",
                "capcode",
                "trip",
                "filename",
                "tim",
                "ext",
                "md5",
                "w",
                "h",
                "tw",
                "th",
                "fsize",
                "country",
                "country_name",
                "board_flag",
                "flag_name"
                "op",
                "replies",
                "images",
                "semantic_url",
                "sticky",
                "closed",
                "archived_on",
                "scraped_on",
                "modified_on",
                "unique_ips",
                "bumplimit",
                "imagelimit",
            )

            item = {
                "board": item.pop("board", ""),
                "thread": item.pop("thread", "")
                if not item.get("resto")
                else item.pop("resto"),
                "id": item.pop("no", ""),
                "unix_timestamp": item.pop("time", ""),
                "etd_timestamp": item.pop("now", ""),
                "author": item.pop("name", ""),
                "post_id": item.pop("id", ""),
                "title": strip_tags(item.pop("sub", "")),
                "body": item.pop("com", ""),
                **{field: item.get(field, "") for field in KNOWN_CHAN_FIELDS},
            }

        return MappedItem(item)

    @staticmethod
    def webjutter_search_request(
        params: dict,
        collection: str,
        url: str,
        user: str,
        password: str,
        max_retries=3,
        timeout=20,
    ) -> dict:
        """Make a request to the search endpoint of Webjutter"""

        if not params:
            raise QueryParametersException("No search query provided.")
        if not collection:
            raise QueryParametersException("No Webjutter collection provided.")
        if not url:
            raise ConfigException("No Webjutter url provided.")
        if not user:
            raise ConfigException("No Webjutter username provided.")
        if not password:
            raise ConfigException("No Webjutter password provided.")

        response = None
        url = f"{url.strip()}/api/{collection.strip()}/search/"
        retries = 0
        max_retries = 3 if max_retries < 1 else max_retries

        while retries <= max_retries:
            try:
                response = requests.post(
                    url, params=params, auth=(user, password), timeout=timeout
                )

                # Check for error status codes before calling raise_for_status()
                if response.status_code >= 400:
                    try:
                        error_data = response.json()
                        # This catches the "message" field sent by validate_elasticsearch_query in views_api.py
                        if isinstance(error_data, dict) and "message" in error_data:
                            raise QueryParametersException(error_data["message"])
                    except (ValueError, json.JSONDecodeError):
                        pass  # Fallback to generic HTTP error handling if not JSON

                response.raise_for_status()
                break

            except Timeout:
                retries += 1
                if retries > max_retries:
                    raise Timeout(
                        f"Request to Webjutter timed out after {timeout} seconds."
                    )
                time.sleep(2)

            except ConnectionError:
                retries += 1
                if retries > max_retries:
                    raise ConnectionError(f"Could not connect to Webjutter at {url}.")
                time.sleep(2)

            except HTTPError as e:
                if response is not None and response.status_code == 429:
                    time.sleep(min(2**retries, 60))
                    retries += 1
                    continue
                # If we already raised QueryParametersException above, let it propagate
                raise e

            except Exception as e:
                if isinstance(e, QueryParametersException):
                    raise e
                raise ConnectionError(
                    f"Unexpected error connecting to Webjutter: {str(e)}"
                )

        if not response:
            raise ConnectionError("Could not get response from Webjutter after multiple attempts.")

        try:
            return response.json()
        except json.JSONDecodeError:
            raise JSONDecodeError("Webjutter returned invalid JSON response.")

    @staticmethod
    def validate_query(query, request, config):
        """Validate Webjutter query."""

        # no query 4 u
        if not query.get("query", "").strip():
            raise QueryParametersException("You must provide a search query.")

        if not query.get("webjutter_datasource", "").strip():
            raise QueryParametersException("You must provide a Webjutter datasource.")

        # Make a test query for query validation and document hits
        url = config.get("webjutter-search.url")
        user = config.get("webjutter-search.user")
        password = config.get("webjutter-search.password")
        collection = query["webjutter_datasource"]
        params = {"q": query.get("query"), "size": 0}

        try:
            response = SearchWebjutter.webjutter_search_request(params, collection, url, user, password, timeout=5, max_retries=1)
        except (ConnectionError, Timeout, RequestException, HTTPError, JSONDecodeError) as e:
            raise QueryParametersException(str(e))

        if not response:
            raise QueryParametersException("Webjutter couldn't respond.")

        total_hits = response["total"]
        if not total_hits:
            raise QueryParametersException("Your search does not result in any results.")
        # Give a warning when we exceed 50,000 hits
        if not query.get("frontend-confirm") and total_hits > 50_000:
            raise QueryNeedsExplicitConfirmationException(f"Your search matches {total_hits:,} items. Do you still want to continue?")

        return query

    def after_process(self):
        """
        Change the datasource type to the one used in the query.

        """
        new_datasource = self.parameters.get("webjutter_datasource", "webjutter")
        self.dataset.change_datasource(
            new_datasource
        )

        super().after_process()