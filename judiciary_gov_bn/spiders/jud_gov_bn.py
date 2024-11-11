from scrapy.cmdline import execute
from datetime import datetime
from typing import Iterable
from scrapy import Request
import pandas as pd
import urllib.parse
import lxml.html
import random
import string
import scrapy
import json
import time
import evpn
import os
import re


def get_years(case_dict: dict) -> str:
    years = case_dict.get('Years', 'N/A')
    years = re.sub(pattern=r'&nbsp;', repl='', string=years).strip()
    return years if years not in ['', ' ', None] else 'N/A'


def get_name(case_dict: dict) -> str:
    name = case_dict.get('Title', 'N/A')
    name = re.sub(pattern=r'&nbsp;', repl='', string=name).strip()
    name = re.sub(pattern=r'&amp;', repl='', string=name).strip()
    return name if name not in ['', ' ', None] else 'N/A'


def get_case_number(case_dict: dict) -> str:
    case_no = case_dict.get('Case_x0020_Number', 'N/A').strip()
    return case_no if case_no not in ['', ' ', None] else 'N/A'


def get_attachment(case_dict: dict) -> str:
    attachment_html = case_dict.get('Attachment', 'N/A')
    attachment_url = 'N/A'
    attachment_html = attachment_html if attachment_html not in ['', ' ', None] else 'N/A'
    if attachment_html != 'N/A':
        selector = lxml.html.fromstring(attachment_html)
        url_slug = ' '.join(selector.xpath('//div//a/@href')).strip()
        attachment_url = 'https://www.judiciary.gov.bn' + url_slug
    return attachment_url if attachment_html not in ['', ' ', None] else 'N/A'


def get_keyword(case_dict: dict) -> str:
    keyword = case_dict.get('Keyword', 'N/A')
    keyword = ' '.join(keyword.split())
    return keyword if keyword not in ['', ' ', None] else 'N/A'


def get_presiding_judge(case_dict: dict) -> str:
    presiding_judge_html = case_dict.get('Presiding_x0020_Judge', 'N/A')
    presiding_judge = 'N/A'
    presiding_judge_html = presiding_judge_html if presiding_judge_html not in ['', ' ', None] else 'N/A'
    if presiding_judge_html != 'N/A':
        selector = lxml.html.fromstring(presiding_judge_html)
        presiding_judge = ' '.join(selector.xpath('//div//text()'))
    presiding_judge = re.sub(pattern=r'\u200b', repl='', string=presiding_judge).strip()
    presiding_judge = re.sub(pattern=r'[^\w\s]', repl='', string=presiding_judge)  # Remove punctuation from each string
    presiding_judge = ' '.join(presiding_judge.split())
    return presiding_judge if presiding_judge not in ['', ' ', None] else 'N/A'


def get_court_title(case_dict: dict) -> str:
    court_title = case_dict.get('Court_x003a_Title', 'N/A')
    return court_title if court_title not in ['', ' ', None] else 'N/A'


def get_jurisdiction_title(case_dict: dict) -> str:
    jurisdiction_title = case_dict.get('Jurisdiction_x003a_Title', 'N/A')
    return jurisdiction_title if jurisdiction_title not in ['', ' ', None] else 'N/A'


def get_title(case_dict: dict, data_dict: dict) -> dict:
    name_raw = get_name(case_dict=case_dict)
    cleaned_titles_list = [name_raw]  # Make sure this list is populated with your titles

    # Regex pattern to match alias phrases
    alias_pattern = r'(?:Also known as|formerly known as|previously known as)\s*([^\)]+)'

    for title_index, title in enumerate(cleaned_titles_list):
        # print('title', title)
        alias_match = re.search(alias_pattern, title, re.IGNORECASE)  # Extract alias using regex
        if alias_match:
            alias = alias_match.group(1).strip()
            # Remove alias from the title value
            title_value = re.sub(pattern=alias_pattern, repl='', string=title, flags=re.IGNORECASE).strip()
            alias_value = alias if alias else 'N/A'
        else:
            title_value = title.strip()
            alias_value = 'N/A'
        title_value = title_value.translate(str.maketrans('', '', string.punctuation))
        alias_value = alias_value.translate(str.maketrans('', '', string.punctuation))

        title_value = re.sub(pattern=r'[^\w\s]', repl='', string=title_value)  # Remove punctuation from each string
        alias_value = re.sub(pattern=r'[^\w\s]', repl='', string=alias_value)  # Remove punctuation from each string

        title_value = ' '.join(title_value.split())
        alias_value = ' '.join(alias_value.split())

        title_indexed_key = f"title_{str(title_index + 1).zfill(2)}"
        alias_indexed_key = f"alias_{str(title_index + 1).zfill(2)}"

        data_dict[title_indexed_key if title_index > 0 else 'title'] = title_value if title not in ['', ' ', None] else 'N/A'
        data_dict[alias_indexed_key if title_index > 0 else 'alias'] = alias_value if alias_value not in ['', ' ', None] else 'N/A'

    return data_dict


class JudGovBnSpider(scrapy.Spider):
    name = "jud_gov_bn"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (BRUNEI)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (BRUNEI)
        self.api.connect(country_id='194')  # brunei country code
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        if self.api.is_connected:
            print('VPN Connected!')
        else:
            print('VPN Not Connected!')

        self.final_data = list()
        self.delivery_date = datetime.now().strftime('%Y%m%d')

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}_{self.delivery_date}.xlsx"  # Filename with Scrape Date

        self.cookies = {
            'WSS_FullScreenMode': 'false',
        }

        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.judiciary.gov.bn',
            'priority': 'u=1, i',
            'referer': 'https://www.judiciary.gov.bn/SJD%20Site%20Pages/Judgment%20Search.aspx',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        # Headers changes at some interval, hence using HeaderGenerator to generate headers
        # self.headers = browserforge.headers.HeaderGenerator().generate()

        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.params = {
            'List': '{6FEDFD3E-8A6B-4718-988B-115E090AA7FA}',
            'View': '{05976A33-BE1E-45B3-BF62-115006D9E3BA}',
            'ViewCount': 262,
            'IsXslView': 'TRUE',
            'IsCSR': 'TRUE',
            'ListViewPageUrl': 'https://www.judiciary.gov.bn/SJD%20Site%20Pages/Judgment%20Search.aspx',
            # 'GroupString': '',
            'IsGroupRender': 'TRUE',
            'WebPartID': '{05976A33-BE1E-45B3-BF62-115006D9E3BA}',
        }

    def start_requests(self) -> Iterable[Request]:
        groupString_list = [
            ";#Court of Appeal;#Court of Appeal - Civil;#", ";#Court of Appeal;#Court of Appeal - Commercial;#",
            ";#Court of Appeal;#Court of Appeal - Criminal;#",
            ";#High Court;#High Court - Civil;#", ";#High Court;#High Court - Commercial;#",
            ";#High Court;#High Court - Criminal;#",
            ";#Intermediate Court;#Intermediate Court - Civil;#", ";#Intermediate Court;#Intermediate Court - Commercial;#",
            ";#Intermediate Court;#Intermediate Court - Criminal;#",
            ";#Magistrate Court;#Magistrate's Court - Civil;#", ";#Magistrate Court;#Magistrate's Court - Commercial;#",
            ";#Magistrate Court;#Magistrate's Court - Criminal;#"
        ]
        for groupString in groupString_list:
            params_copy = self.params.copy()
            params_copy['GroupString'] = groupString
            url = 'https://www.judiciary.gov.bn/_layouts/15/inplview.aspx?' + urllib.parse.urlencode(params_copy)
            yield scrapy.Request(url=url, cookies=self.cookies, headers=self.headers, method='POST', meta={'impersonate': random.choice(self.browsers)}, callback=self.parse, dont_filter=True, cb_kwargs={'groupString': groupString, 'params': params_copy})

    def parse(self, response, **kwargs):
        json_dict = json.loads(response.text)
        groupString = kwargs['groupString']
        params = kwargs['params']
        # print(f'Processing group: {groupString}')
        # print(f'Current params: {params}')
        cases_list: list = json_dict.get('Row', [])
        if cases_list:
            self.process_data(cases_list=cases_list)

        # Perform Pagination if Given Next Page URL
        nextHref = json_dict.get('NextHref', 'NA')
        print('-' * 50)

        if nextHref != 'NA':
            parsed_url = urllib.parse.urlparse(nextHref)  # Parse the URL
            qp = urllib.parse.parse_qs(parsed_url.query)  # Extract query parameters
            query_params = {key: value[0] for key, value in qp.items()}  # Convert query_params to a flat dictionary
            query_params['View'] = params['View']
            params.update(query_params)  # Update the existing params with the new ones from nextHref
            new_query_string = urllib.parse.urlencode(params)  # Construct the new URL with the updated params
            url = 'https://www.judiciary.gov.bn/_layouts/15/inplview.aspx?' + new_query_string

            yield scrapy.Request(url=url, cookies=self.cookies, headers=self.headers, method='POST', meta={'impersonate': random.choice(self.browsers)},
                                 callback=self.parse, dont_filter=True, cb_kwargs={'groupString': groupString, 'params': params})

    def process_data(self, cases_list: list):
        for case_dict in cases_list:
            data_dict: dict = {
                'url': 'https://www.judiciary.gov.bn/SJD%20Site%20Pages/Judgment%20Search.aspx',
                'years': get_years(case_dict=case_dict),  # Years
                # 'name': get_name(case_dict=case_dict),  # Name
                'case_number': get_case_number(case_dict=case_dict),  # Case Number
                'attachment': get_attachment(case_dict=case_dict),  # Attachment URL
                'keyword': get_keyword(case_dict=case_dict),  # Keyword
                'presiding_judge': get_presiding_judge(case_dict=case_dict),  # Presiding Judge
                'court_title': get_court_title(case_dict=case_dict),  # Court Title
                'jurisdiction_title': get_jurisdiction_title(case_dict=case_dict),  # Jurisdiction Title
            }
            # 'name': get_name(case_dict=case_dict),  # Name
            data_dict = get_title(case_dict=case_dict, data_dict=data_dict)

            print('data_dict', data_dict)
            self.final_data.append(data_dict)
            print('++++++++++')

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data)
            priority_columns = ["url", "title", "alias", "case_number", "attachment"]
            columns_required = priority_columns + [col for col in data_df.columns if col not in priority_columns]
            data_df = data_df[columns_required]
            # data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter') as writer:
                data_df.to_excel(excel_writer=writer, index=False)
            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()


if __name__ == '__main__':
    execute(f'scrapy crawl {JudGovBnSpider.name}'.split())
