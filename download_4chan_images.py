"""
Download images linked in dataset
"""
import requests
import json
import shutil
import time

from lxml import etree
from lxml.cssselect import CSSSelector as css
from io import StringIO

from common.lib.helpers import UserInput
from common.lib.dataset import DataSet
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
    type = "image-downloader-4chan"  # job type ID
    category = "Visual"  # category
    title = "Download 4chan images"  # title displayed in UI
    description = "Download images and store in a a zip file. May take a while to complete as images are retrieved " \
                  "externally. Note that not always all images can be saved. For imgur galleries, only the first " \
                  "image is saved. For animations (GIFs), only the first frame is saved if available. A JSON metadata file " \
                  "is included in the output archive.\n4chan datasets should include the image_md5 column."  # description displayed in UI
    extension = "zip"  # extension of result file, used internally and in UI
    media_type = "image"  # media type of the dataset

    config = {
        "image-downloader.max": {
            "type": UserInput.OPTION_TEXT,
            "coerce_type": int,
            "default": 1000,
            "help": "Max images to download",
            "tooltip": "Only allow downloading up to this many images per batch. Increasing this can easily lead to "
                       "very long-running processors and large datasets. Set to 0 for no limit."
        }
    }

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        """
        Get processor options

        This method by default returns the class's "options" attribute, or an
        empty dictionary. It can be redefined by processors that need more
        fine-grained options, e.g. in cases where the availability of options
        is partially determined by the parent dataset's parameters.

        :param DataSet parent_dataset:  An object representing the dataset that
        the processor would be run on
        :param config:  Configuration reader
        """

        fourplebs_boards = ["adv", "f", "hr", "mlpol", "mo", "o", "pol", "s4s", "sp", "tg", "trv", "tv", "x"]

        options = {
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "default": 100,
            },
            "chan_archives": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Archive to get images from",
                "options": {
                    "desuarchive": "desuarchive.org",
                    "fourplebs": "4plebs.org"
                },
                "default": "fourplebs" if parent_dataset and parent_dataset.parameters.get("board") in fourplebs_boards else "desuarchive",
                "tooltip": "Selecting more archives will slow down this processor. Try to identify which archive hosts "
                           "your board(s).",
            }
        }

        # Update the amount max and help from config
        max_number_images = int(config.get('image-downloader.max', 1000))
        if max_number_images != 0:
            options['amount']['help'] = f"No. of images (max {max_number_images})"
            options['amount']['max'] = max_number_images
            options['amount']['min'] = 1
        else:
            # 0 can download all images
            options['amount']['help'] = "No. of images"
            options['amount']["min"] = 0
            options['amount']["tooltip"] = "Set to 0 to download all images"

        return options

    @classmethod
    def is_compatible_with(cls, module=None, config=None):
        """
        Allow processor on top image rankings

        :param module: Dataset or processor to determine compatibility with
        """
        return module.parameters.get("datasource") == "fourchan"

    def process(self):
        """
        This takes a 4chan as input, and outputs a zip file with
        images along with a file, .metadata.json, that contains identifying
        information.
        """
        amount = self.parameters.get("amount", 100)

        if amount == 0:
            amount = self.config.get('image-downloader.max', 1000)

        # is there anything for us to download?
        if self.source_dataset.num_rows == 0:
            self.dataset.update_status("No images to download.", is_final=True)
            self.dataset.finish(0)
            return

        external_url = self.parameters.get("chan_archives")
        if not external_url:
            self.dataset.update_status("No images to download.", is_final=True)
            self.dataset.finish(0)
            return

        external_url = "https://archive.4plebs.org" if external_url == "fourplebs" else "https://desuarchive.org"

        # prepare
        search_urls = set()
        self.staging_area = self.dataset.get_staging_area()
        self.complete = False

        # first, get URLs to download images from
        self.dataset.update_status("Reading source file")
        item_index = 0
        self.filenames = {}

        for item in self.source_dataset.iterate_items(self):
            # note that we do not check if the amount of URLs exceeds the max
            # `amount` of images; images may fail, so the limit is on the
            # amount of downloaded images, not the amount of potentially
            # downloadable image URLs

            item_index += 1

            if item_index % 50 == 0:
                self.dataset.update_status("Extracting image links from item %i/%i" % (item_index, self.source_dataset.num_rows))

            if not item["md5"]:
                continue
            if item.get("ext") in ["mp4", ".webm"]:
                continue

            remote_path = f"{external_url}/_/search/image/{item["md5"].replace("/", "_")}"
            search_urls.add(remote_path)

        if not search_urls:
            self.dataset.update_status("No image urls identified.", is_final=True)
            self.dataset.finish(0)
            return
        else:
            self.dataset.log('Collected %i image urls.' % len(search_urls))

        # next, loop through files and download them - until we have as many files
        # as required. Note that files that cannot be downloaded or parsed do
        # not count towards that limit
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0"
        downloaded_files = set()
        failures = []
        metadata = {}

        image_urls = set()
        retry_counts = {}  # Track retry attempts per URL

        # We need the actual CDN urls for these images, using the search endpoint of our Fuuka archive
        for search_url, response in self.iterate_proxied_requests(
                search_urls,
                preserve_order=False,
                headers={"User-Agent": ua},
                verify=False,
                timeout=20
        ):
            if self.interrupted:
                raise ProcessorInterruptedException()

            failure = False
            if type(response) is FailedProxiedRequest:
                if type(response.context) is requests.exceptions.Timeout:
                    self.dataset.log(f"Error: Timeout while trying to open {search_url}: {response.context}")
                elif type(response.context) is requests.exceptions.SSLError:
                    self.dataset.log(f"Error: SSL Error for URL {search_url}: {response.context}")
                elif type(response.context) is requests.exceptions.ConnectionError:
                    self.dataset.log(f"Error: Connection Error for URL {search_url}: {response.context}")
                else:
                    self.dataset.log(f"Error: Error for URL {search_url}: {response.context}")

                failure = True

            if not failure:
                # Get the URL link
                try:
                    parser = etree.HTMLParser()
                    tree = etree.parse(StringIO(response.content.decode("utf-8")), parser)
                    image_url = css("a.thread_image_link")(tree)[0].get("href")
                except IndexError as e:
                    # Check if no results alert found
                    if css("div.alert")(tree):
                        if "No results found" in ' '.join([i for i in css("div.alert")(tree)[0].itertext()]):
                            self.dataset.log("Image not found: %s" % search_url)
                            continue  # Skip this URL, move to next

                    # Potential rate limiting - retry up to 3 times
                    retry_count = retry_counts.get(search_url, 0)
                    if retry_count < 3:
                        retry_counts[search_url] = retry_count + 1
                        sleep_time = 2 ** (retry_count + 1)  # Exponential backoff: 2, 4, 8 seconds
                        self.dataset.log(
                            f"IndexError for {search_url}, retrying ({retry_counts[search_url]}/3) after {sleep_time}s sleep...")
                        time.sleep(sleep_time)

                        # Re-queue the URL at the end to try again
                        self.push_proxied_request(
                            search_url,
                            position=-1,
                            headers={"User-Agent": ua},
                            verify=False,
                            timeout=20
                        )
                        continue  # Skip to next URL in iteration
                    else:
                        # Max retries reached
                        self.dataset.log(
                            "IndexError while trying to download 4chan image %s after 3 retries: %s" % (search_url, e))
                        continue

                except UnicodeDecodeError:
                    self.dataset.log("4chan image search could not be completed for image %s, skipping" % search_url)
                    continue

                self.filenames[image_url] = image_url.split("/")[-1]

                self.dataset.update_status(f"Retrieved {len(self.filenames)}/{amount} image URLs from {external_url}")
                self.dataset.update_progress(len(self.filenames) / len(search_urls) / 2)

                # We're assuming we can download all images here
                if len(self.filenames) >= amount and amount > 0:
                    break

        # So we don't retrieve more image links
        self.flush_proxied_requests()

        # Now use the actual image URL for proxied requests
        max_images = min(len(self.filenames), amount) if amount > 0 else len(self.filenames)
        self.dataset.log(f"Starting download of up to {max_images:,} image(s).")
        for image_url, response in self.iterate_proxied_requests(
                self.filenames.keys(),
                preserve_order=False,
                headers={"User-Agent": ua},
                hooks={
                    # use hooks to download the content (stream=True) in parallel
                    "response": self.stream_url
                },
                verify=False,
                timeout=20,
                stream=True,
        ):
            downloaded_file = self.staging_area.joinpath(self.filenames[image_url])
            failure = False

            if self.interrupted:
                self.completed = True
                self.flush_proxied_requests()
                shutil.rmtree(self.staging_area)
                raise ProcessorInterruptedException()

            elif response.status_code != 200:
                self.dataset.log(
                    f"Error: File not found (status {response.status_code}) at {image_url}"
                )
                failure = True

            if not failure:
                if len(downloaded_files) < amount or amount == 0:
                    downloaded_files.add(image_url)
                    self.dataset.update_status(
                        f"Downloaded {len(downloaded_files):,} of {max_images:,} file(s)"
                    )
                    self.dataset.update_progress(.5 + (len(downloaded_files) / max_images / 2))

                if len(downloaded_files) >= amount and amount != 0:
                    # parallel requests may still be running so halt these
                    # before ending the loop and wrapping up
                    self.complete = True
            else:
                failures.append(image_url)
                downloaded_file.unlink(missing_ok=True)

            metadata[image_url] = {
                "filename": self.filenames[image_url],
                "url": image_url,
                "success": not failure,
                "from_dataset": self.source_dataset.key
            }

            if self.complete:
                break

        with self.staging_area.joinpath(".metadata.json").open(
                "w", encoding="utf-8"
        ) as outfile:
            json.dump(metadata, outfile)

        # delete supernumerary partially downloaded files
        self.flush_proxied_requests()  # get rid of remaining queue

        for url, filename in self.filenames.items():
            url_file = self.staging_area.joinpath(filename)
            if url_file.exists() and url not in downloaded_files:
                url_file.unlink()

        # finish up
        self.dataset.update_progress(1.0)
        self.write_archive_and_finish(
            self.staging_area, len([x for x in metadata.values() if x.get("success")])
        )

    def stream_url(self, response, fourcat_original_url=None, *args, **kwargs):
        """
        Helper function for iterate_proxied_requests

        Simply streams data from a request; allows for stream=True with a
        request, meaning we do not need to buffer everything in memory.

        :param requests.Response response: requests response object
        """
        if fourcat_original_url is None:
            raise KeyError("Missing fourcat_original_url for response hook; proxied requests must pass it")

        # Strict requirement: we must have a filename for the original URL
        original_url = fourcat_original_url
        if original_url not in self.filenames:
            raise KeyError(
                f"Missing filename for original request URL: {original_url}"
            )

        destination = self.staging_area.joinpath(self.filenames[original_url])

        while chunk := response.raw.read(1024, decode_content=True):
            if not response.ok or self.interrupted or self.complete:
                # stop reading when request is bad, or we have enough already
                # try to make the request finish ASAP so it can be cleaned up
                response._content_consumed = True
                response._content = False
                response.raw.close()
                return

            with destination.open("ab") as outfile:
                try:
                    outfile.write(chunk)
                except FileNotFoundError:
                    # this can happen if processing finished *after* the while
                    # loop started (i.e. self.complete flipped); safe to ignore
                    # in that case
                    pass

    @staticmethod
    def map_metadata(url, data):
        """
        Iterator to yield modified metadata for CSV

        :param str url:  string that may contain URLs
        :param dict data:  dictionary with metadata collected previously
        :yield dict:  	  iterator containing reformated metadata
        """
        row = {
            "url": url,
            "number_of_posts_with_url": len(data.get("post_ids", [])),
            "post_ids": ", ".join(data.get("post_ids", [])),
            "filename": data.get("filename"),
            "download_successful": data.get('success', "")
        }

        yield row
