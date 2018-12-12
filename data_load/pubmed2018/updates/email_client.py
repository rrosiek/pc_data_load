import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

import datetime

from data_load.base.utils.data_loader_utils import DataLoaderUtils
import data_load.base.utils.doc_utils as doc_utils
from config import *

import os
import data_load.base.utils.file_utils as file_utils

from_address = "ocat.niaid@gmail.com"
password = "kittyC@t"
smtpserver = 'smtp.gmail.com:587'

COMMASPACE = ', '

def get_title(pmid):
    field_paths = ['MedlineCitation.Article.ArticleTitle']
    data_loader_utils = DataLoaderUtils(SERVER, INDEX, TYPE)

    doc = data_loader_utils.fetch_doc(pmid)

    title = ''
    if doc is not None:
        title = doc_utils.extract_title_for_doc_and_index(doc, field_paths)

    return title


def send_notification_for_prospect(prospect):
    # Recipient's email address
    to_address = prospect['email']

    # if to_address == 'admin@altum.com':
    #     to_address = 'robint@qburst.com'
    to_address = ['robint@qburst.com']

    # Hard coded for testing
    # to_address = 'robint@qburst.com'
    docs_with_matching_citations = prospect['docs_with_matching_citations']

    print 'Sending mail for prospect', to_address, 'docs_with_matching_citations',  len(docs_with_matching_citations)

    now = datetime.datetime.now()
    local_date = now.strftime("%m-%d-%Y")

    html = """\
            <head>
                <style>
                    @import url(https://fonts.googleapis.com/css?family=Hind|Hind-Semibold);
                </style>
            </head>

            <body style="color:#9ca4ab;font-family:Hind,Helvetica,sans-serif;margin:20px 10px 40px 10px;">
                <div style="text-align:center;margin-top:40px;padding-bottom:10px;">
                    <img src=\"cid:image\">
                </div>
                <div style="margin-top:30px;margin-bottom:10px;vertical-align:center;font-size:25px;color:#9984f0;font-weight:bold;text-align:center">
                    Prospective Citation Notification: """ + local_date + """
                </div>
                <div style="font-weight:bold;font-size:20px;;margin-top:30px;">
                    The following new articles reference designated prospective articles:
                </div>
            """

    for doc in docs_with_matching_citations:
        pmid = doc['_id']
        title = get_title(pmid)

        html += """
                <div style="margin-top:30px;">
                    <div style="font-size:15px;">
                        <div style="font-weight:bold;color:#3b445e">
                            <a href='https://www.ncbi.nlm.nih.gov/pubmed/""" + pmid + """' target="_blank">PMID """ + pmid + """</a>
                        </div>
                        <div style="font-weight:bold;margin-top:10px;color:#9ca4ab;">
                            &quot;""" + title + """&quot;
                        </div>
                        <div style="font-weight:bold;color:#9984f0;margin-top:15px;">
                            Cites these articles marked for notification:
                        </div>
                    </div>
                    <ul style="font-size:13px;list-style:none;margin-top:20px;">
                """
        prospective_cites = doc['matching_citations']
        for prospective_cite in prospective_cites:
            prospective_cite_title = get_title(prospective_cite)

            html += """
                        <li style="margin-top:15px;">
                            <div style="font-weight:bold;color:#3b445e">
                                <a href='https://www.ncbi.nlm.nih.gov/pubmed/""" + prospective_cite + """' target="_blank" >PMID """ + prospective_cite + """</a>
                            </div>
                            <div style="font-weight:bold;margin-top:5px;color:#9ca4ab;">
                                &quot;""" + prospective_cite_title + """&quot;
                            </div>
                        </li>
                    """

        html += """
                    </ul>
                </div>
                """

    html += """
            </body>
            """

    html = html.encode('utf-8')
    html_part = MIMEText(html, 'html')

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "OCAT Update Notification " + str(local_date)
    msg['From'] = "OCAT NIAID <" + from_address + ">"
    msg['To'] = to_address

    fp = open('niaid-logo.jpg', 'rb')
    msg_image = MIMEImage(fp.read())
    fp.close()
    msg_image.add_header('Content-ID', '<image>')
    msg.attach(msg_image)

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(html_part)

    # Send the message via local SMTP server.
    s = smtplib.SMTP(smtpserver)
    s.starttls()
    s.login(from_address, password)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    problems = s.sendmail(from_address, to_address, msg.as_string())
    s.quit()

    print 'Sending notification to', to_address
    print 'Problems', problems

    return problems

def send_update_start_notification(local_date, update_files):
    # to_address = ['robint@qburst.com', 'darryl.blackburn@altum.com']
    # to_address = file_utils.load_file('', 'update_status_emails.json')
    to_address = ['robint@qburst.com']
    # now = datetime.datetime.now()
    # local_date = now.strftime("%m-%d-%Y")

    html = "<h3> OCAT PubMed Auto Update Started: " + local_date + "</h3><br/>"
    html += "<b>Processing " +  str(len(update_files)) + " update files</b><br/>"
    html += """<div style="margin-left:20px;">"""

    for update_file in update_files:
        html += "<span>" + update_file + "</span><br/>"

    html += """</div>"""

    html_part = MIMEText(html, 'html')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = "OCAT Auto Update Status " + str(local_date)
    msg['From'] = "OCAT NIAID <" + from_address + ">"
    msg['To'] = COMMASPACE.join(to_address)

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(html_part)

    # Send the message via local SMTP server.
    s = smtplib.SMTP(smtpserver)
    s.starttls()
    s.login(from_address, password)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    problems = s.sendmail(from_address, to_address, msg.as_string())
    s.quit()

    return problems

def send_update_notifications(local_date, update_data, all_prospects):
    # to_address = ['robint@qburst.com', 'darryl.blackburn@altum.com']
    # to_address = file_utils.load_file('', 'update_status_emails.json')
    to_address = ['robint@qburst.com']

    # , 'darryl.blackburn@altum.com'
    new_udate_file_count = len(update_data)
    html = ""

    html += """<h3><span>OCAT PubMed Auto Update </span><span style="font-weight: normal;">""" + local_date + """</span></h3>
    <div style="margin-bottom: 10px; font-size: 13px; font-weight: bold;">
        <span style="color: #000;"> Total update files processed: </span>
        <span>""" + str("{:,}".format(new_udate_file_count)) + """</span>
    </div>"""

    if new_udate_file_count > 0:
        html += """<table style="width:100%; border: 1px solid #AAA; border-collapse: collapse;">
            <tr style="text-align: left;
                padding: 4px;
                color: #000;
                font-size: 13px;  border-collapse: collapse;">
                <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">File name</th>
                <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">File path</th>
                <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">Total articles</th>
                <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">New articles</th>
                <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">Updated articles</th>
            </tr>"""

        for update_file_path in update_data:
            update_file_name = os.path.basename(update_file_path)
            update_data_for_file = update_data[update_file_path]
            articles_processed = len(update_data_for_file['articles_processed'])
            new_articles = len(update_data_for_file['new_articles'])
            # updated_articles = len(update_data_for_file['updated_articles'])
            updated_articles = articles_processed - new_articles
            
            html += """<tr style="text-align: left;
                color: #000;
                font-size: 13px;  border-collapse: collapse;">
                <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                    <b>""" + update_file_name + """</b>
                </td>
                <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                    """ + update_file_path + """
                </td>
                <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                    """ + str("{:,}".format(articles_processed)) + """
                </td>
                <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                    """ + str("{:,}".format(new_articles)) + """
                </td>
                <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                    """ + str("{:,}".format(updated_articles)) + """
                </td>
            </tr>"""

        html += """</table>"""

        html += """<div style="margin-top: 20px; margin-bottom: 10px; font-size: 13px; font-weight: bold;">
            <span style="color: #000;">New PMIDs referencing prospective articles: </span>"""
        if len(all_prospects) == 0:
            html += """<b>0</b>"""
        html += """</div>"""

        if len(all_prospects) > 0:
            html += """<table style="width:100%; border: 1px solid #AAA; border-collapse: collapse;"">
                <tr style="text-align: left;
                padding: 4px;
                color: #000;
                font-size: 13px;  border-collapse: collapse;">
                    <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                        User
                    </th>
                    <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                        Updated PMID
                    </th>
                    <th style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                        Referenced Articles
                    </th>
                </tr>"""

            for prospect in all_prospects:
                # Recipient's email address
                new_email = True
                email = prospect['email']
                docs_with_matching_citations = prospect['docs_with_matching_citations']
                for doc in docs_with_matching_citations:
                    pmid = doc['_id']
                    prospective_cites = doc['matching_citations']
                    prospective_cites_string = ', '.join(prospective_cites) 

                    html += """<tr style="text-align: left;
                        padding: 4px;
                        color: #000;
                        font-size: 13px;  border-collapse: collapse;">"""
                    if new_email:
                        html += """<td  style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;" rowspan=" """ + str(len(docs_with_matching_citations)) + """ ">
                            <b>""" + email + """</b>
                        </td>"""
                        new_email = False
                    html += """<td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                            """ + str(pmid) + """
                        </td>
                        <td style="border: 1px solid #AAA; border-collapse: collapse; padding: 4px;">
                            """ + prospective_cites_string + """
                        </td>       
                    </tr>"""

            html += """</table>"""

    html_part = MIMEText(html, 'html')

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "OCAT Auto Update Status " + str(local_date)
    msg['From'] = "OCAT NIAID <" + from_address + ">"
    msg['To'] = COMMASPACE.join(to_address)

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(html_part)

    # Send the message via local SMTP server.
    s = smtplib.SMTP(smtpserver)
    s.starttls()
    s.login(from_address, password)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    problems = s.sendmail(from_address, to_address, msg.as_string())
    s.quit()

    print 'Sending update status mail; problems', problems
    return problems


