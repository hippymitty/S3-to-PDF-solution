import json
import boto3
import io
import datetime
import parse
import re
import tempfile
import numpy as np
import pandas as pd
import xlrd
import zipfile
import matplotlib.style
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from pandas.plotting import table
import logging

mpl.style.use('ggplot')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    for record in event['Records']:
        with tempfile.NamedTemporaryFile() as temporary_pdf_buffer:
            with PdfPages(temporary_pdf_buffer) as pdf_pages:

                # Get File from S3
                report_file_buffer = get_file_from_s3_and_return_as_buffer(
                    bucket=record['s3']['bucket']['name'],
                    key=record['s3']['object']['key']
                )

                CLIENT_NAME = get_client_name(key)
                DATE = datetime.datetime.now().strftime("%Y%m%d")
                unzip_and_process(pdf_pages, '/tmp/Data.zip')

            # Upload generated report to S3
            temporary_pdf_buffer.seek(0)
            boto3.client("s3").upload_fileobj(temporary_pdf_buffer, record['s3']['bucket']['name'], "reports/" + CLIENT_NAME + "_" + DATE + "_output.pdf")

def get_client_name(s3_key):
    return s3_key.replace('upload/', '').replace('.zip', '')

def get_file_from_s3_and_return_as_buffer(bucket, key):
        #return boto3.resource('s3').Object(bucket, key).get()['Body']
        boto3.client('s3').download_file(bucket, key, '/tmp/Data.zip')


def unzip_and_process(pdf_pages, zip_location):
    #change file extension as required, this is for 3 different files to be ingested and processed
    with zipfile.ZipFile(zip_location) as zipper:
        with zipper.open('Data/datafileOne.csv') as fileOne:
            fileOne_plot(pdf_pages, fileOne)
        with zipper.open('Data/datafileTwo.csv') as fileTwo:
            fileTwo_plot(pdf_pages, fileTwo)
        with zipper.open('Data/datafileThree.xlsx') as fileThree:
            fileThree_plot(pdf_pages, fileThree)

def fileOne_plot(pdf, fileOne_csv_file_buffer):
     fileOne_csv = pd.read_csv(fileOne_csv_file_buffer)
     fileOne_dataframe = pd.DataFrame(fileOne_csv)
     fileOne_data = fileOne_dataframe[["fileOne", "ColName"]]
     fileOne_data['fileOne'] = fileOne_data['fileOne'].str.replace('%', '').astype(float)
     fileOne_data['ColName'] = fileOne_data['ColName'].astype('str')
     #two plots, plot one
     grid_size = (5, 2)
     ax1 = plt.subplot2grid(grid_size, (2, 0), rowspan=4, colspan=1)
     fileOne_data.groupby(['ColName'])['fileOne'].plot.bar(ax=ax1)
     plt.title('Title')
     plt.xlabel("X lable")
     plt.ylabel("Y label")
     plt.grid(True)
     plt.tight_layout()
     # plot table, plot two
     ax2 = plt.subplot2grid(grid_size, (2, 1), rowspan=4, colspan=2)
     plt.axis('off')
     tbl = table(ax2, fileOne_data)
     tbl.auto_set_font_size(True)
     plt.grid(True)
     plt.tight_layout()
     pdf.savefig()
     plt.close()

def fileTwo_plot(pdf, fileTwo_csv_file_buffer):
     fileTwodata = pd.read_csv(fileTwo_csv_file_buffer)
     fileTwo_dataframe = pd.DataFrame(fileTwodata)
     fileTwo_data = fileTwo_dataframe[["Col1", "Col2", "Col3"]]
     grid_size = (5, 2)
     ax = plt.subplot2grid(grid_size, (0, 0), rowspan=1, colspan=3)
     plt.axis('off')
     tbl = table(ax, fileTwo_data)
     tbl.auto_set_font_size(True)
     plt.tight_layout()
     pdf.savefig()

def fileThree_plot(pdf, fileThree_csv_file_buffer):
     xlsx_data = pd.read_excel(fileThree_csv_file_buffer)
     fileThree_dataframe = pd.DataFrame(xlsx_data)
     fileThree_data = fileThree_dataframe[["Col1", "Col2", "Col3", "Col4"]]
    
     #Data missing for required chart so chart not added
        
     #calculate time difference & add to new column
     fileThree_data['NewCol'] = fileThree_data['Col3'] - fileThree_data['Col2']
     print(fileThree_data.dtypes)
        
     #split data out
     #Creates new columns and splits date/time
     Col2_datetime = pd.DatetimeIndex(fileThree_data['Col2'])
     fileThree_data['Col2Date'] = Col2_datetime.date
     fileThree_data['Col2Time'] = Col2_datetime.time
        
     Col3_datetime = pd.DatetimeIndex(fileThree_data['Col3'])
     fileThree_data['Col3Date'] = Col3_datetime.date
     fileThree_data['Col3Time'] = Col3_datetime.time
        
     #FileThree plot
     restime = fileThree_data['NewCol']
     resolution_time = restime.dt.days
     Col2_date = pd.to_datetime(fileThree_data["Col2Date"]) #convert object to datetime
     Col2_day = Col2_date.dt.day
     Col3_date = pd.to_datetime(fileThree_data["Col3Date"]) #convert object to datetime
     Col3_day = Col3_date.dt.day
     fig4 = plt.figure()
     ax = plt.subplot()
     plt.bar(Col2_date.values, resolution_time.values)
     ax.xaxis.set_major_locator(mdates.WeekdayLocator())
     ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
     plt.title('Title')
     plt.xlabel("X label")
     plt.ylabel("Y label")
     plt.tight_layout()
     pdf.savefig(fig4)
        
     #Comparison charts
     Col3_count = fileThree_data.ColName.count() #count of items in Col3, change ColName to relevant column name
     Col2_count = fileThree_data.ColName.count() #count the items in Col2,  change ColName to relevant column name
        
     #comparison chart pt1
     ax1 = plt.subplot()
     fileThree_data.groupby(['Col2Date'])['Col2'].agg('count').plot(legend=False, ax=ax1, marker='o', label='Add label')
     ax1.xaxis.set_major_locator(mdates.WeekdayLocator())
     ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
     plt.title('Title')
     plt.xlabel("X label")
     plt.ylabel("Y label")
     ax1.legend(loc='best')
     plt.tight_layout()

     #comparison chart pt2
     ax2 = plt.subplot()
     fileThree_data.groupby(['Col3Date'])['Col2'].agg('count').plot(legend=False, ax=ax2, marker='o', label='Add label')
     ax2.xaxis.set_major_locator(mdates.WeekdayLocator())
     ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
     plt.title('Title')
     plt.xlabel("X label")
     plt.ylabel("Y label")
     ax2.legend(loc='best')
     plt.tight_layout()
     pdf.savefig()

     #Average Chart
     count = resolution_time.count()
     average = resolution_time.mean()
        
     fig7 = plt.figure()
     ax = plt.subplot()
     fileThree_data.groupby(['Col2Date'])['Timing'].agg('sum').plot(ax=ax, linewidth=4)
     ax.xaxis.set_major_locator(mdates.WeekdayLocator())
     ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
     plt.title('Title)
     plt.xlabel("X label")
     plt.ylabel("Y label")
     plt.tight_layout()
     pdf.savefig(fig7)

     # Severity chart
     fig8 = plt.figure()
     fig8, ax = plt.subplots()
     for key, grp in fileThree_data.groupby(['Col1']):
         ax = grp.plot(ax=ax, kind='line', x='Col2Date', y='Timing', label=key, marker='o')
     ax.xaxis.set_major_locator(mdates.WeekdayLocator())
     ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
     plt.title('Title')
     plt.xlabel('X label')
     plt.ylabel('Y label')
     plt.legend(loc='best')
     plt.tight_layout()
     pdf.savefig()
     plt.close()