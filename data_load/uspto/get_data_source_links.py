from pyquery import PyQuery
import re

DATA_SOURCE_DOMAIN  = 'http://patents.reedtech.com/'
DATA_SOURCE_URL = DATA_SOURCE_DOMAIN + 'pgrbft.php'

URL_PATTERN = r'downloads\/[A-Z]\w+\/([0-9]{4})\/([a-z, 0-9,._]+)'

POST_2001_FILE_NAME_PATTERN = r'i?pg([0-9]{6}).zip'
PRE_2001_FILE_NAME_PATTERN = r'pftaps([0-9]{8})_(wk[0-9]{2}).zip'

html = """
<div class="bulkyear" id="2019">2019 [<a href="#top">Top</a>]</div>
<table class="bulktable"><tbody><tr><td width="200"><b>File Name</b></td><td width="200"><b>File Size (bytes)</b></td><td width="200"><b>Posting Date</b></td></tr>
<tr><td><a href="downloads/GrantRedBookText/2019/ipg190122.zip">ipg190122.zip</a></td><td>110,127,823</td><td>01/22/2019</td></tr>
<tr><td><a href="downloads/GrantRedBookText/2019/ipg190115.zip">ipg190115.zip</a></td><td>76,080,930</td><td>01/15/2019</td></tr>
<tr><td><a href="downloads/GrantRedBookText/2019/ipg190108.zip">ipg190108.zip</a></td><td>137,681,748</td><td>01/08/2019</td></tr>
<tr><td><a href="downloads/GrantRedBookText/2019/ipg190101.zip">ipg190101.zip</a></td><td>130,778,992</td><td>01/01/2019</td></tr>
</tbody></table>"""

# ALL_YEARS = []

ALL_FILE_URLS = []
FILES_PER_YEAR = {}

def process_table(index, element):
    file_url = element.get('href')
    file_url = DATA_SOURCE_DOMAIN + file_url
    print index, file_url
    ALL_FILE_URLS.append(file_url)

# def process_year(index, element):
#     year = element.get('id')
#     print index, year
#     ALL_YEARS.append(year)

def sort_files():
    print 'Sorting files..'

    for file_url in ALL_FILE_URLS:
        result = re.search(URL_PATTERN, file_url)   
        if result is not None:
            year = result.group(1)
            file_name = result.group(2)

            print year, file_name

            if year not in FILES_PER_YEAR:
                FILES_PER_YEAR[year] = []

            FILES_PER_YEAR[year].append(file_url)

    for year in FILES_PER_YEAR:
        files_per_year = FILES_PER_YEAR[year]
        files_per_year.sort()
        FILES_PER_YEAR[year] = files_per_year

def generate_files_per_year():
    if len(FILES_PER_YEAR) == 0:
        print 'Loading', DATA_SOURCE_URL
        pq = PyQuery(url=DATA_SOURCE_URL)
        # pq = PyQuery(html)

        # year_divs = pq('div').filter('.bulkyear')
        data_tables = pq('table').filter('.bulktable').find('a')

        # print year_divs
        # print 'Years'
        # year_divs.each(process_year)

        print 'Files'
        data_tables.each(process_table)

        sort_files()

def get_files(pre_2001=False):
    generate_files_per_year()

    filtered_files_per_year = {}
     
    years = FILES_PER_YEAR.keys()
    years.sort()

    for year in years:
        if pre_2001:
            if int(year) < 2001:  
                print year, len(FILES_PER_YEAR[year]), 'files'
                filtered_files_per_year[year] = filter_file_names(FILES_PER_YEAR[year], pre_2001=pre_2001)
        else:
            if int(year) >= 2001:
                print year, len(FILES_PER_YEAR[year]), 'files'
                filtered_files_per_year[year] = filter_file_names(FILES_PER_YEAR[year], pre_2001=pre_2001)
                
    return filtered_files_per_year


def filter_file_names(file_urls, pre_2001=False):
    filtered_urls = []
    for file_url in file_urls:
        result = re.search(URL_PATTERN, file_url)   
        if result is not None:
            file_name = result.group(2)

            pattern = POST_2001_FILE_NAME_PATTERN
            if pre_2001:
                pattern = PRE_2001_FILE_NAME_PATTERN
            if re.match(pattern, file_name):
                filtered_urls.append(file_url)

    return filtered_urls

def run():
    generate_files_per_year()

    filtered_files_per_year = {}
     
    years = FILES_PER_YEAR.keys()
    years.sort()

    for year in years:
        print year, len(FILES_PER_YEAR[year]), 'files'
        filtered_files_per_year[year] = FILES_PER_YEAR[year]
        
    return filtered_files_per_year

# run()




