from data_load.base.data_mapper import DataMapper
import datetime
import re

class PubmedDataMapper(DataMapper):

    @staticmethod
    def allow_doc_creation(data_source_name):
        return True

    @staticmethod
    def create_only(data_source_name):
        return False

    @staticmethod
    def get_es_id(_id):
        return _id

    @staticmethod
    def get_doc_id(_id):
        return _id

    @staticmethod
    def pad_zeros(number, count):
        number_str = str(number)
        while len(number_str) < count:
            number_str = '0' + number_str

        return number_str

    @staticmethod
    def get_date_revised(doc):
        date_revised = None
        if 'MedlineCitation' in doc:
            if 'DateRevised' in doc['MedlineCitation']:
                try:
                    dr = doc['MedlineCitation']['DateRevised']
                    year = int(dr['Year'])
                    month = int(dr['Month'])
                    day = int(dr['Day'])
                    date_revised = datetime.date(year=year, month=month, day=day)
                except Exception as e:
                    print 'Getting date revised', e
        return date_revised


    @staticmethod
    def get_latest_data_item(data):
        # for data_item in data:
        latest_date_revised = None
        latest_doc = None
        for doc in data:
            date_revised = PubmedDataMapper.get_date_revised(doc)
            if date_revised is not None:
                if latest_date_revised is None or date_revised >= latest_date_revised:
                    latest_date_revised = date_revised
                    latest_doc = doc
            
        if latest_doc is None:
            latest_doc = data[-1]

        return latest_doc

    @staticmethod
    def get_citations(data):
        doc = PubmedDataMapper.get_latest_data_item(data)

        citations = []
        if 'PubmedData' in doc:
            if 'ReferenceList' in doc['PubmedData']:
                if 'Reference' in doc['PubmedData']['ReferenceList']:
                    reference_list = doc['PubmedData']['ReferenceList']['Reference']

                    if not isinstance(reference_list, list):
                        reference_list = [reference_list]

                    for reference in reference_list:
                        if 'ArticleIdList' in reference:
                            article_id_list = reference['ArticleIdList']
                            if 'ArticleId' in article_id_list:
                                article_ids = article_id_list['ArticleId']
                                if not isinstance(article_ids, list):
                                    article_ids = [article_ids]

                                for article_id in article_ids:
                                    if 'IdType' in article_id:
                                        article_id_type = article_id['IdType']
                                        if article_id_type == 'pubmed':
                                            pmid = article_id['content']
                                            citations.append(pmid)

        return citations

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        new_doc = PubmedDataMapper.create_doc(_id, data_source_name, data)

        update_doc = {}
        for key in new_doc:
            new_value = new_doc[key]
            if new_value is not None and len(new_value) > 0:
                update_doc[key] = new_value



        return update_doc

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}
        data = PubmedDataMapper.get_latest_data_item(data)

        try:
            # Pub Date
            pub_date = PubmedDataMapper.extract_pub_date(data)
            if pub_date is not None:
                doc['startJournalDate'] = pub_date['year'] + '-' + pub_date['month'] + '-' + pub_date['day']
                doc['startDateYear'] = pub_date['year']

            # Medline Citation
            if 'MedlineCitation' in data:
                doc['MedlineCitation'] = PubmedDataMapper.clean_medline_citation(data['MedlineCitation'])

            if 'PubmedData' in data:
                doc['PubmedData'] = data['PubmedData']

            # Authors
            doc['Author_Details'] = PubmedDataMapper.extract_authors_list(data)

            now = datetime.datetime.now()
            updated_date = now.isoformat()

            citations = PubmedDataMapper.get_citations([data])
            citations_history_item = {
                'citations': citations,
                'data_source': data_source_name,
                'updated_date': updated_date
            }
            doc['citations_history'] = [citations_history_item]
        except Exception as e:
            print 'Create doc:', e

        return doc

    @staticmethod
    def clean_abstract_text(abstract_text):
        cleaned_abstract_text = []

        if isinstance(abstract_text, dict):
            abstract_item = {
                'content': '',
                'Label': ''
            }
            if 'content' in abstract_text:
                abstract_item['content'] = abstract_text['content']

            if 'Label' in abstract_text:
                abstract_item['Label'] = abstract_text['Label']

            cleaned_abstract_text.append(abstract_item)
        elif isinstance(abstract_text, list):
            for abstract_text_item in abstract_text:
                abstract_item = {
                    'content': '',
                    'Label': ''
                }
                if isinstance(abstract_text_item, dict):
                    if 'content' in abstract_text_item:
                        abstract_item['content'] = abstract_text_item['content']

                    if 'Label' in abstract_text_item:
                        abstract_item['Label'] = abstract_text_item['Label']
                else:
                    abstract_item['content'] = abstract_text_item

                cleaned_abstract_text.append(abstract_item)
        else:
            abstract_item = {
                'content': abstract_text,
                'Label': ''
            }
            cleaned_abstract_text.append(abstract_item)

        return cleaned_abstract_text

    @staticmethod
    def clean_medline_citation(medline_citation):
        if 'OtherAbstract' in medline_citation:
            # if 'AbstractText' in medline_citation['OtherAbstract']:
            #     abstract_text = medline_citation['OtherAbstract']['AbstractText']
            #     medline_citation['OtherAbstract']['AbstractTextObj'] = PubmedDataMapper.clean_abstract_text(abstract_text)
            #     medline_citation['OtherAbstract'].pop('AbstractText', None)
            medline_citation.pop('OtherAbstract', None)

        if 'CoiStatement' in medline_citation:
                coi_statement = medline_citation['CoiStatement']
                content = ''
                if isinstance(coi_statement, dict):
                    if 'content' in coi_statement:
                        content = coi_statement['content']
                else:
                    content = coi_statement

                medline_citation['CoiStatement'] = content    

        if 'Article' in medline_citation:
            if 'Abstract' in medline_citation['Article']:
                if 'AbstractText' in medline_citation['Article']['Abstract']:
                    abstract_text = medline_citation['Article']['Abstract']['AbstractText']
                    medline_citation['Article']['Abstract']['AbstractText'] = PubmedDataMapper.clean_abstract_text(abstract_text)

            if 'ArticleTitle' in medline_citation['Article']:
                article_title = medline_citation['Article']['ArticleTitle']
                content = ''
                if isinstance(article_title, dict):
                    if 'content' in article_title:
                        content = article_title['content']
                else:
                    content = article_title

                medline_citation['Article']['ArticleTitle'] = content

            if 'VernacularTitle' in medline_citation['Article']:
                vernacular_title = medline_citation['Article']['VernacularTitle']
                content = ''
                if isinstance(vernacular_title, dict):
                    if 'content' in vernacular_title:
                        content = vernacular_title['content']
                else:
                    content = vernacular_title

                medline_citation['Article']['VernacularTitle'] = content                     

        return medline_citation

    @staticmethod
    def get_month_digit(month_or_season):
        lookup = {
            "winter": 1,
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "spring": 4,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "summer": 7,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "autumn": 10,
            "fall": 10,
            "oct": 10,
            "nov": 11,
            "dec": 12
        }

        month_or_season = month_or_season.lower()

        if month_or_season in lookup:
            return lookup[month_or_season]

        return None

    @staticmethod
    def extract_authors_list(doc):
        authors_list = []

        if 'MedlineCitation' in doc:
            if 'Article' in doc['MedlineCitation']:
                if 'AuthorList' in doc['MedlineCitation']['Article']:
                    if 'Author' in doc['MedlineCitation']['Article']['AuthorList']:
                        authors = doc['MedlineCitation']['Article']['AuthorList']['Author']

                        if isinstance(authors, list):
                            for author_dict in authors:
                                author_details = {}

                                author_details['CollectiveName'] = ''
                                author_details['ForeName'] = ''
                                author_details['LastName'] = ''
                                author_details['Initials'] = ''

                                if 'CollectiveName' in author_dict:
                                    author_details['CollectiveName'] = author_dict['CollectiveName']
                                if 'ForeName' in author_dict:
                                    author_details['ForeName'] = author_dict['ForeName']
                                if 'LastName' in author_dict:
                                    author_details['LastName'] = author_dict['LastName']
                                if 'Initials' in author_dict:
                                    author_details['Initials'] = author_dict['Initials']

                                authors_list.append(author_details)
                        else:
                            author_details = {}
                            author_dict = authors

                            author_details['CollectiveName'] = ''
                            author_details['ForeName'] = ''
                            author_details['LastName'] = ''
                            author_details['Initials'] = ''

                            if 'CollectiveName' in author_dict:
                                author_details['CollectiveName'] = author_dict['CollectiveName']
                            if 'ForeName' in author_dict:
                                author_details['ForeName'] = author_dict['ForeName']
                            if 'LastName' in author_dict:
                                author_details['LastName'] = author_dict['LastName']
                            if 'Initials' in author_dict:
                                author_details['Initials'] = author_dict['Initials']

                            authors_list.append(author_details)

        return authors_list


    @staticmethod
    def extract_pub_date(doc):
        if 'MedlineCitation' in doc:
            if 'Article' in doc['MedlineCitation']:
                if 'Journal' in doc['MedlineCitation']['Article']:
                    if 'JournalIssue' in doc['MedlineCitation']['Article']['Journal']:
                        if 'PubDate' in doc['MedlineCitation']['Article']['Journal']['JournalIssue']:
                            pub_date = doc['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']

                            try:
                                # Pub Date Formats
                                # {'Month': 'May', 'Year': '2017'},
                                # {'Month': 'Aug', 'Day': '25', 'Year': '2016'},
                                # {'Year': '2016'},
                                # {'MedlineDate': '2016 Nov - Dec'},
                                # {'MedlineDate': 'Summer 2008'},
                                # {'MedlineDate': '2016 Nov/Dec'}
                                # {'MedlineDate': '8/15/12'},
                                # {'Season': 'Fall', 'Year': '2017'}
                                year = None
                                month = 1
                                day = 1

                                if 'Year' in pub_date:
                                    year = pub_date['Year']

                                if 'Month' in pub_date:
                                    month_str = pub_date['Month']
                                    month = PubmedDataMapper.get_month_digit(month_str)

                                if 'Season' in pub_date:
                                    season = pub_date['Season']
                                    month = PubmedDataMapper.get_month_digit(season)

                                if 'Day' in pub_date:
                                    day = pub_date['Day']

                                if 'MedlineDate' in pub_date:
                                    medline_date = pub_date['MedlineDate']

                                    # {'MedlineDate': '2016 Nov - Dec'},
                                    # {'MedlineDate': '2016 Nov/Dec'}
                                    pattern1 = '([0-9]{4}) +([A-Za-z]{3}) {0,}-?\/? {0,}([A-Za-z]{3})'
                                    result = re.match(pattern1, medline_date)
                                    if result is not None:
                                        year = result.group(1)
                                        month_str = result.group(2)
                                        month = PubmedDataMapper.get_month_digit(month_str)
                                    else:
                                        # {'MedlineDate': 'Summer 2008'},
                                        pattern2 = '([A-Za-z]\w+) +([0-9]{4})'
                                        result = re.match(pattern2, medline_date)
                                        if result is not None:
                                            season = result.group(1)
                                            year = result.group(2)
                                            month = PubmedDataMapper.get_month_digit(season)
                                        else:
                                            # {'MedlineDate': '8/15/12'},
                                            pattern3 = '([0-9]{1,2}) {0,}(?:\/|-) {0,}([0-9]{1,2}) {0,}(?:\/|-) {0,}([0-9]{2,4})'
                                            result = re.match(pattern3, medline_date)
                                            if result is not None:
                                                month = result.group(1)
                                                day = result.group(2)
                                                year = result.group(3)

                                                if len(str(year)) == 2:
                                                    year = '20' + str(year) # not accurate
                                            else:
                                                # {'MedlineDate':'1978-1979'}
                                                pattern4 = '([0-9]{4}) {0,}(?:\/|-) {0,}([0-9]{4})'
                                                result = re.match(pattern4, medline_date)
                                                if result is not None:
                                                    year = result.group(1)

                                if month is not None:
                                    try:
                                        month = PubmedDataMapper.pad_zeros(month, 2)
                                    except Exception as e:
                                        pass
                                else:
                                    month = '01'

                                if day is not None and int(day) >= 1:
                                    try:
                                        day = PubmedDataMapper.pad_zeros(day, 2)
                                    except Exception as e:
                                        pass
                                else:
                                    day = '01'

                                if year is not None:
                                    return {
                                        'year': str(year),
                                        'month':  str(month),
                                        'day':  str(day)
                                    }
                            except Exception as e:
                                print 'Getting pub date:', e, pub_date

        return None

    