"""
Download 4chan images from external arrchives
"""

import json
import shutil
import time
import cloudscraper

from common.lib.helpers import UserInput
from backend.lib.processor import BasicProcessor
from backend.lib.proxied_requests import FailedProxiedRequest
from common.lib.exceptions import ProcessorInterruptedException, FourcatException

class InvalidDownloadedFileException(FourcatException):
    pass

__author__ = "Sal Hagen"
__credits__ = ["Sal Hagen"]
__maintainer__ = "Sal Hagen"
__email__ = "4cat@oilab.eu"


class FourchanSearchImageDownloader(BasicProcessor):
    """
    Image downloader
    Downloads top images and saves as zip archive
    """
    type = "image-downloader-4chan"
    category = "Visual"
    title = "Download 4chan images from archives"
    description = ("Download images from external archives and save them as a ZIP file. This first queries the 4plebs "
                   "or desuarchive API to collect image URLs. For best results, choose an archive that hosts the "
                   "board used by this dataset. Some images may not be available on the selected archive, so the "
                   "final image count may be lower than the maximum requested. The ZIP file includes a JSON "
                   "metadata file with more information."
    )
    extension = "zip"
    media_type = "image"

    config = {
        "image-downloader.max": {
            "type": UserInput.OPTION_TEXT,
            "coerce_type": int,
            "default": 1000,
            "help": "Max images to download",
            "tooltip": "Set to 0 for no limit. High numbers increase processing time significantly."
        }
    }

    # Centralized configuration for archives
    ARCHIVE_CONFIG = {
        "fourplebs": {
            "api_base": "https://archive.4plebs.org",
            "cdn_pattern": "https://i.4pcdn.org/{board}/{filename}",
            "boards": ["adv", "f", "hr", "mlpol", "mo", "o", "pol", "s4s", "sp", "tg", "trv", "tv", "x"]
        },
        "desuarchive": {
            "api_base": "https://desuarchive.org",
            "cdn_pattern": "https://desu-usergeneratedcontent.xyz/{board}/image/{filename}",
            "boards": ["a", "aco", "an", "c", "cgl", "co", "d", "fit", "g", "his", "int", "k", "m", "mlp",
                       "mu", "q", "qa", "r9k", "tg", "trash", "vr", "wsg"]
        }
    }

    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0"

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        board = parent_dataset.parameters.get("board") if parent_dataset else None

        # Determine default archive based on board
        default_archive = "desuarchive"
        if board in cls.ARCHIVE_CONFIG["fourplebs"]["boards"]:
            default_archive = "fourplebs"

        options = {
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "default": 100,
            },
            "archive": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Archive to get images from",
                "options": {
                    "desuarchive": "desuarchive.org",
                    "fourplebs": "4plebs.org"
                },
                "default": default_archive,
                "tooltip": "Selecting the wrong archive for your board will result in no images.",
            }
        }

        # Update the amount max and help from config
        max_number_images = int(config.get('image-downloader.max', 1000))
        if max_number_images != 0:
            options['amount']['help'] = f"No. of images (max {max_number_images})"
            options['amount']['max'] = max_number_images
            options['amount']['min'] = 1
        else:
            options['amount']['help'] = "No. of images"
            options['amount']["min"] = 0
            options['amount']["tooltip"] = "Set to 0 to download all images"

        return options

    @classmethod
    def is_compatible_with(cls, module=None, config=None):
        return module.parameters.get("datasource") == "fourchan"

    def process(self):
        """Main execution flow."""
        self.amount = self.parameters.get("amount", 100)
        if self.amount == 0:
            self.amount = self.config.get('image-downloader.max', 1000)

        if self.source_dataset.num_rows == 0:
            self.dataset.update_status("No images to download.", is_final=True)
            self.dataset.finish(0)
            return

        self.archive_choice = self.parameters.get("archive")
        if not self.archive_choice:
            self.dataset.update_status("No archive selected.", is_final=True)
            self.dataset.finish(0)
            return

        # Setup state
        self.staging_area = self.dataset.get_staging_area()
        self.complete = False
        self.filenames = {} # Maps URL -> Filename

        # Get Image URLs from source dataset through the chan archive APIs
        api_urls = self.get_api_urls()
        if not api_urls:
            self.dataset.update_status("No archive API urls found in the dataset to get the image URLs.", is_final=True)
            self.dataset.finish(0)
            return

        self.dataset.update_status(f'Collected {len(api_urls)} search urls.')

        # Query APIs to get actual image URLs. This often differs with the `tim` field in the source dataset,
        # so this step is necessary to get a full list.
        self.collect_image_urls(api_urls)

        if not self.filenames:
            self.dataset.update_status("No image URLs found after API search.", is_final=True)
            self.dataset.finish(0)
            return

        # Download the actual images
        self.download_images()

    def get_api_urls(self):
        """Iterates the source dataset to build API search URLs."""
        self.dataset.update_status("Reading source file")
        search_urls = set()

        # Load config for the selected archive
        archive_config = self.ARCHIVE_CONFIG[self.archive_choice]
        api_base = archive_config["api_base"]
        supported_boards = archive_config["boards"]

        item_index = 0
        for item in self.source_dataset.iterate_items(self):
            item_index += 1
            if item_index % 50 == 0:
                self.dataset.update_status(f"Extracting URLs from item {item_index}/{self.source_dataset.num_rows}")

            if not item.get("md5"):
                continue
            if item.get("ext") in [".mp4", ".webm"]:
                continue

            # Determine if we use direct post lookup or MD5 search
            # If the board is supported by the selected archive, use direct post lookup
            if item["board"] in supported_boards:
                url = f"{api_base}/_/api/chan/post/?board={item['board']}&num={item['id']}"
            else:
                # Fallback to searching desuarchive by MD5 for cross-archive hits?
                # (Preserving original logic: search by MD5 if board not in list)
                url = f"https://desuarchive.org/_/api/chan/search/?image={item['md5']}"

            search_urls.add(url)

        return search_urls

    def extract_url_from_json(self, response_json, search_url):
        """
        Parses the API JSON to find the image URL.
        Returns: (image_url, should_retry, retry_reason)
        """
        cdn_pattern = self.ARCHIVE_CONFIG[self.archive_choice]["cdn_pattern"]

        # Check for API Rate Limits
        if "error" in response_json and "limit exceeded" in response_json.get("error", ""):
            return None, True, "API limit exceeded"

        # Normalize response structure (search vs post lookup)
        # If result is { "0": { "posts": [...] } }
        if "0" in response_json and "posts" in response_json["0"]:
            data = response_json["0"]["posts"][0]
        else:
            data = response_json

        # Extract Media Link
        if "media" in data:
            if "media_link" in data["media"]:
                return data["media"]["media_link"], False, None
            else:
                # Construct from filename pattern
                image_name = data["media"]["media"]
                # Extract board from search_url parameters
                try:
                    board_param = [p for p in search_url.split("?")[1].split("&") if p.startswith("board=")][0]
                    board = board_param.split("=")[1]
                    return cdn_pattern.format(board=board, filename=image_name), False, None
                except IndexError:
                    pass

        return None, False, "Missing media data"

    def collect_image_urls(self, search_urls):
        """
        Queries the archive APIs to get image URLs from API requests.
        Handles the difference between Cloudscraper (4plebs) and ProxiedRequests (Desu).
        """
        self.dataset.update_status("Collecting image URLs from API")
        retry_counts = {}

        # Strategy 1: Cloudscraper (4plebs)
        if self.archive_choice == "fourplebs":
            self.dataset.update_status("Using cloudscraper for 4plebs API requests")
            scraper = cloudscraper.create_scraper(browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False})

            for search_url in search_urls:
                if self.interrupted: raise ProcessorInterruptedException()

                # Manual retry loop for synchronous scraper
                while True:
                    should_retry = False

                    try:
                        resp = scraper.get(search_url, timeout=20)
                        if resp.status_code != 200:
                            retry_reason = f"API {resp.status_code}"
                        else:
                            img_url, should_retry, retry_reason = self.extract_url_from_json(resp.json(), search_url)
                            if img_url:
                                self.filenames[img_url] = img_url.split("/")[-1]
                                break # Success
                    except Exception as e:
                        retry_reason = f"Exception: {e}"

                    # Handle retry Logic
                    if should_retry or (retry_reason and "API" in retry_reason):
                        count = retry_counts.get(search_url, 0)
                        if count < 3:
                            retry_counts[search_url] = count + 1
                            sleep_time = 2 ** (count + 1)
                            self.dataset.update_status(
                                f"Retrying {search_url} ({count + 1}/3): {retry_reason}"
                            )
                            time.sleep(sleep_time)
                            continue

                    break  # Stop retrying if success or max retries reached or fatal error

                time.sleep(1)  # Rate limiting 4plebs
                self.dataset.update_status(f"Retrieved {len(self.filenames)}/{self.amount} image URLs")
                self.dataset.update_progress(len(self.filenames) / self.amount / 2)
                if len(self.filenames) >= self.amount > 0:
                    break

        # Strategy 2: Proxied Requests (Desuarchive)
        else:
            for search_url, response in self.iterate_proxied_requests(
                search_urls, preserve_order=False, headers={"User-Agent": self.UA},
                verify=False, timeout=20
            ):
                if self.interrupted: raise ProcessorInterruptedException()

                should_retry = False
                retry_reason = ""

                # Check Network Errors
                if isinstance(response, FailedProxiedRequest):
                    should_retry = False # ProxiedRequest usually exhausts its own internal retries
                    self.dataset.update_status(f"Network error for {search_url}: {response.context}")
                elif response.status_code != 200:
                    retry_reason = f"API {response.status_code}"
                else:
                    try:
                        img_url, should_retry, retry_reason = self.extract_url_from_json(response.json(), search_url)
                        if img_url:
                            self.filenames[img_url] = img_url.split("/")[-1]
                    except ValueError:
                        self.dataset.update_status(f"JSON parse failed for {search_url}")

                # Handle Retry Logic (Re-queueing)
                if should_retry:
                    count = retry_counts.get(search_url, 0)
                    if count < 3:
                        retry_counts[search_url] = count + 1
                        sleep_time = 2 ** (count + 1)
                        time.sleep(sleep_time)  # Simple sleep before re-queue

                        self.push_proxied_request(
                            search_url,
                            position=-1,
                            headers={"User-Agent": self.UA},
                            verify=False,
                            timeout=20,
                        )
                        continue

                time.sleep(0.5)
                self.dataset.update_status(f"Retrieved {len(self.filenames)}/{self.amount} image URLs")
                self.dataset.update_progress(len(self.filenames) / self.amount / 2)
                if len(self.filenames) >= self.amount > 0:
                    break

            # Cleanup
            self.flush_proxied_requests()

    def download_images(self):
        """Downloads the files in self.filenames."""
        downloaded_files = set()
        failures = []
        metadata = {}

        # Limit the input list to the max amount to avoid unnecessary queueing
        targets = list(self.filenames.keys())
        if self.amount > 0:
            targets = targets[:self.amount]

        self.dataset.update_status(f"Getting {len(targets):,} image(s).")

        for image_url, response in self.iterate_proxied_requests(
                targets,
                preserve_order=False,
                headers={"User-Agent": self.UA},
                hooks={"response": self.stream_url},
                verify=False,
                timeout=20,
                stream=True,
        ):
            if self.interrupted:
                self.flush_proxied_requests()
                shutil.rmtree(self.staging_area)
                raise ProcessorInterruptedException()

            success = False
            local_filename = self.filenames[image_url]
            downloaded_path = self.staging_area.joinpath(local_filename)

            if response.status_code == 200:
                downloaded_files.add(image_url)
                success = True
            else:
                failures.append(image_url)
                downloaded_path.unlink(missing_ok=True)
                self.dataset.update_status(f"Error: status {response.status_code} at {image_url}")

            metadata[image_url] = {
                "filename": local_filename,
                "url": image_url,
                "success": success,
                "from_dataset": self.source_dataset.key
            }

            self.dataset.update_status(f"Downloaded {len(downloaded_files):,} file(s)")
            self.dataset.update_progress(0.5 + (len(downloaded_files) / len(targets) / 2))

            if self.amount > 0 and len(downloaded_files) >= self.amount:
                self.complete = True
                break

        # Finalize
        with self.staging_area.joinpath(".metadata.json").open("w", encoding="utf-8") as outfile:
            json.dump(metadata, outfile)

        self.flush_proxied_requests()

        # Cleanup unused files
        for url, filename in self.filenames.items():
            fpath = self.staging_area.joinpath(filename)
            if fpath.exists() and url not in downloaded_files:
                fpath.unlink()

        self.dataset.update_progress(1.0)
        self.write_archive_and_finish(
            self.staging_area, len([x for x in metadata.values() if x.get("success")])
        )

    def stream_url(self, response, fourcat_original_url=None, *args, **kwargs):
        """Helper to stream response content to disk."""
        if fourcat_original_url is None:
            raise KeyError("Missing fourcat_original_url for response hook")

        original_url = fourcat_original_url
        if original_url not in self.filenames:
            # Should not happen if logic is correct
            raise KeyError(f"Missing filename for: {original_url}")

        destination = self.staging_area.joinpath(self.filenames[original_url])

        while chunk := response.raw.read(1024, decode_content=True):
            if not response.ok or self.interrupted or self.complete:
                response._content_consumed = True
                response.raw.close()
                return

            with destination.open("ab") as outfile:
                outfile.write(chunk)

    @staticmethod
    def map_metadata(url, data):
        """Iterator to yield modified metadata for CSV"""
        yield {
            "url": url,
            "number_of_posts_with_url": len(data.get("post_ids", [])),
            "post_ids": ", ".join(data.get("post_ids", [])),
            "filename": data.get("filename"),
            "download_successful": data.get('success', "")
        }