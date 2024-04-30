import requests

import bs4
import pandas as pd
from typing import List


class WaterUFONet:
    """
    Class to scrape data from the waterufo.net website archival.

    Credit to the late Carl Feindt for his work cataloging UFO sightings over water.
    """

    def __init__(self):
        # base url for waterufo
        self.url = "http://www.waterufo.net/2012/search.php?txtSearch=all"

        # available snapshot dates
        self.snapshot_dates = [
            "20130902194856",
            "20140916023935",
            "20150507185325",
            "20150908120644",
            "20160904220814",
            "20171112174144",
            "20181221230434",
            "20191010100225",
            "20191022061839",
        ]

        # web archive snapshots
        self.snapshots = [
            f"https://web.archive.org/web/{dt}/{self.url}" for dt in self.snapshot_dates
        ]

    def process_snapshot(self, snapshot: str) -> pd.DataFrame:
        """
        Processes a snapshot of the archived waterufo.net website and extract the case
        table data / relevant links.
        """
        # fetch data from the archive snapshot
        try:
            response = requests.get(snapshot)
            response.raise_for_status()

        except Exception as e:
            print(f"Error fetching data from {snapshot}!\n")
            print(e)
            return None

        # parse the html content using beautifulsoup
        soup = bs4.BeautifulSoup(response.content, "html.parser")

        # extract case table and rows
        try:
            table = soup.find_all("table")[4]
            rows = table.find_all("tr")
        except IndexError:
            print("Error parsing case table! Format may have changed.")
            return None

        # build case table data for downstream dataframe
        table_data = []
        for row in rows[1:]:
            columns = row.find_all("td")
            row_data = []
            for column in columns:
                row_data.append(column.text.strip())
            table_data.append(row_data)

        # add links to individual cases
        anchors = soup.find_all("a")
        links = [
            a["href"]
            for a in anchors
            if a.get("alt") is not None and a["alt"].startswith("View the Report")
        ]

        if not links:
            print("No links found in the case table! Format may have changed.")
            return None

        # build resulting dataframe from table data and add links
        result = pd.DataFrame(table_data)

        try:
            result["link"] = [f"https://web.archive.org/{l}" for l in links]

        except Exception as e:
            print("Error adding links to dataframe!")
            print(e)
            return None

        return result

    def run(self, n_snapshots: int = 1) -> List[pd.DataFrame]:
        """
        Run the scraper and process the specified number of snapshots.
        """
        frames = []
        try:
            for snapshot in self.snapshots[:n_snapshots]:
                df = self.process_snapshot(snapshot)
                if df is not None:
                    frames.append(df)
        except Exception as e:
            print(f"Error processing snapshots!")
            print(e)

        return frames
