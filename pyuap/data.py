import requests

import bs4
import pandas as pd
from typing import List
import os
from pathlib import Path
import time
from openai import OpenAI
import arrow


class WaterUFONet:
    """
    Class to scrape data from the waterufo.net website archival.

    Credit to the late Carl Feindt for his work cataloging UFO sightings over water.
    """

    def __init__(self, max_failures: int = 10, buffer_time: float = 10):
        if buffer_time < 10:
            raise ValueError("Buffer time must be at least 10 seconds.")
        # base url for waterufo
        self.url = "http://www.waterufo.net/2012/search.php?txtSearch=all"

        # available snapshot dates
        self.snapshot_dates = [
            "20191022061839",
            "20191010100225",
            "20181221230434",
            "20171112174144",
            "20160904220814",
            "20150908120644",
            "20150507185325",
            "20140916023935",
            "20130902194856",
        ]

        # web archive snapshots
        self.snapshots = [
            f"https://web.archive.org/web/{dt}/{self.url}" for dt in self.snapshot_dates
        ]

        self.max_failures = max_failures
        self.buffer_time = buffer_time

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

    def get_case_tables(self, n_snapshots: int = 1) -> List[pd.DataFrame]:
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

    def get_case_report(self, link: str) -> str:
        """
        Get the full case report from the archived link.
        """
        time.sleep(self.buffer_time)
        response = requests.get(link)
        response.raise_for_status()
        soup = bs4.BeautifulSoup(response.content, "html.parser")
        paras = soup.find_all("p")
        txt = ""
        for p in paras:
            txt += (
                p.get_text().replace("\xa0", "").replace("\n", "").replace("\x96", "")
            )

        return txt

    def get_case_reports(self) -> List[dict]:
        """
        Get the full case reports for all cases in the archive.
        """
        case_tables = self.get_case_tables(n_snapshots=1)
        start_time = arrow.now()
        total_time, failures, results = 0, 0, []
        for table in case_tables:
            table_links = table["link"]
            for idx, link in enumerate(table_links):
                report_start_time = arrow.now()
                try:
                    report = self.get_case_report(link)
                except Exception as e:
                    print(f"Error fetching report from {link}. Skipping to next.")
                    print(e)
                    failures += 1
                    if failures >= self.max_failures:
                        print(f"Max failures reached. Exiting.")
                        return results
                    continue

                report_end_time = arrow.now()
                report_time = (
                    report_end_time - report_start_time
                ).total_seconds() - self.buffer_time
                total_time += report_time

                average_time_per_report = total_time / (idx + 1)
                remaining_reports = len(table_links) - (idx + 1)

                expected_end_time = report_end_time.shift(
                    seconds=(average_time_per_report + self.buffer_time)
                    * remaining_reports
                )
                print(
                    f"Processed Report: {idx + 1} / {len(table_links)}, average time: {average_time_per_report:.2f} seconds, "
                    f"Expected end time: {expected_end_time.format('YYYY-MM-DD HH:mm:ss')}"
                )
                results.append({"link": link, "report": report})

        end_time = arrow.now()
        total_time = (end_time - start_time).total_seconds()
        print(f"Total time taken: {total_time:.2f} seconds")
        return results


class FAADroneSightings:
    def __init__(self, buffer_time: int = 10):
        if buffer_time < 10:
            raise ValueError(
                "Buffer time must be at least 10 seconds for this scraper."
            )

        self.buffer_time = buffer_time
        self.base = "https://www.faa.gov"

        self.url = (
            "https://www.faa.gov/uas/resources/public_records/uas_sightings_report"
        )

    def get_file_links(self):
        response = requests.get(self.url)
        response.raise_for_status()
        soup = bs4.BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a")
        file_links = [
            self.base + link["href"]
            for link in links
            if link.text.startswith("Reported UAS Sightings")
        ]
        return file_links

    def download_files(self, path: str = "data"):
        file_links = self.get_file_links()
        for idx, link in enumerate(file_links):
            response = requests.get(link)
            response.raise_for_status()
            ftype = link.split(".")[-1]
            if not ftype.startswith("x"):
                print(f"Cannot download link {link}. Skipping.")
                continue
            pth = Path(os.getcwd()) / path / f"uas_sightings_report_{idx}.{ftype}"
            with open(pth, "wb") as f:
                f.write(response.content)
            print(f"Downloaded file: {pth}")
            time.sleep(self.buffer_time)
        print("Download complete.")

    def read_files(self, path: str = "data") -> pd.DataFrame:
        pth = Path(os.getcwd()) / path
        files = list(pth.glob("*.xlsx"))
        if not files:
            print("No files found in the specified directory.")
            return None

        dfs = []
        for file in files:
            try:
                df = pd.read_excel(file, engine="openpyxl")
                dfs.append(df)
            except Exception as e:
                print(f"Error reading file {file}. Skipping.")
                print(e)
                continue

        if not dfs:
            print("No files successfully read.")
            return None

        try:
            df = pd.concat(dfs, axis=0).reset_index(drop=True)
        except Exception as e:
            print("Error concatenating dataframes.")
            print(e)
            return None

        return df

    def sample_summaries(self, df: pd.DataFrame, n: int = 5) -> List[str]:
        return [s for s in df["Summary"].dropna().sample(n).values]

    def extract_jsons(
        self,
        summaries: List[str],
        model: str = "gpt-3.5-turbo-0125",
        choices: int = 1,
        system_prompts: List[str] = None,
    ) -> List[dict]:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        if system_prompts is None:
            system_prompts = [
                {
                    "role": "system",
                    "content": "You are a helpful assistant designed to output JSON.",
                },
                {
                    "role": "system",
                    "content": "Respond with your analysis in JSON format.",
                },
                {
                    "role": "system",
                    "content": "The JSON schema should include {'altitude': int, 'pilot_report': boolean, 'evasive_action': boolean}.",
                },
                {
                    "role": "system",
                    "content": "You are an expert at assessing object altitude from potentially ambiguous text.",
                },
                {
                    "role": "system",
                    "content": "Pay special attention to any mentions of FEET or ALTITUDE in the summary.",
                },
                {
                    "role": "system",
                    "content": "You will be shown a series of report summaries from the FAA Drone Sightings dataset.",
                },
                {
                    "role": "system",
                    "content": "For each summary you are tasked with estimating the altitude of the unidentified object in question.",
                },
                {
                    "role": "system",
                    "content": "Sometimes there will be multiple altitudes mentioned and it's critical you extract the most relevant one for the unidentified object.",
                },
                {
                    "role": "system",
                    "content": "Most of the summaries are from the pilots themselves but occasionally it's from the flight control operator or other sources.",
                },
                {
                    "role": "system",
                    "content": "The JSON output length must match the number of summaries shown.",
                },
            ]

        summary_prompts = [
            {
                "role": "user",
                "content": f"Here are {len(summaries)} summaries from the FAA Drone Sightings dataset.",
            }
        ]

        for summary in summaries:
            summary_prompts.append({"role": "user", "content": summary})

        summary_prompts.append(
            {
                "role": "user",
                "content": "Take a deep breath and solve the problem step by step. It's important you get this right.",
            }
        )

        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=system_prompts + summary_prompts,
            n=choices,
        )

        return [c.message.content for c in response.choices]
