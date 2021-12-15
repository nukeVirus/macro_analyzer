import os
import time
from datetime import date
from datetime import datetime
import pandas as pd
import shutil
import logging
import argparse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from time import sleep
import subprocess
import urllib.request


load_dotenv()


def _parse_arguments():
    """A parser to capture changes on the table were the data is stored or the path where the csv file is located."""

    parser = argparse.ArgumentParser()

    parser.add_argument("--PROD",
                        help="Change SIDs from users into production",
                        default=False,
                        action="store_true"
                        )
    return parser.parse_args()


class UsaTrades:
    # DB variables
    load_dotenv()
    DB_USER = os.getenv('DB_USER')
    DB_PWD = os.getenv('DB_PWD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    # Paths
    URL = 'https://www.census.gov/retail/mrts/www/statedata/state_retail_yy.csv'
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\cpi_data"
    PATH_TO_CHROME = os.getenv('WEBDRIVER_PATH')


    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    def remove_dir(self):
        """Removes the main directory and its contents if it is present"""
        try:
            shutil.rmtree(f'{self.FILE_PATH}')
            logging.info("Existing main folder removed")
        except Exception as e:
            logging.error(e)
            logging.info("No existing main folder found")

    def ensure_dir(self):
        """Function to create folder(if not present) to store the initial data downloaded from the source"""
        try:
            logging.info("Creating new folder to download the data")
            subprocess.check_call(f'mkdir {self.FILE_PATH}', shell=True)
        except Exception as e:
            logging.error(e)
            logging.info("Failed to create new folder to download the data")

    def download(self):

        """This is for download the csv"""


    def extract(self):
        """Function to extract the data from the unzipped .csv file."""
        extracted_df=pd.read_csv(self.URL)
        time.sleep(10)
        return extracted_df

    def transform(self, process_df):
        process_df = process_df.drop('fips', axis = 1)
        process_df = process_df[process_df['stateabbr'] != 'USA']
        process_df = process_df[process_df['naics'] == 'TOTAL'].reset_index(drop = True)
        process_df = process_df.drop(['naics'], axis = 1)
        process_df = process_df.rename(columns = {'stateabbr': 'State Code'})
        state_df = pd.read_excel(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\State_Lookup.xlsx')
        state_df = state_df[['State', 'State Code', 'Region']]
        df_merge = pd.merge(process_df, state_df, on = 'State Code')
        df_merge = df_merge.drop(['State Code'], axis=1)
        melt_df = pd.melt(df_merge, id_vars=['State', 'Region'], var_name='Date', value_name='Value')
        remove_yy = lambda data : data.split('y')[-1]
        add_hyphen = lambda data : data[0:4] + "-" + data[4:]
        melt_df['Date'] = melt_df['Date'].map(remove_yy).map(add_hyphen)
        melt_df['Date'] = pd.to_datetime(melt_df['Date'])
        melt_df['Data Element'] = "RETAIL SALES"
        melt_df['Description'] = "AVERAGE RETAIL SALES"
        melt_df['Frequency'] = "MONTHLY"
        final_df = melt_df[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Region', 'Description']]
        final_df['Value'] = final_df['Value'].astype(float)
        final_df.to_csv(self.FILE_PATH+'\\retail_sales.csv')
        return final_df


    def db_conn(self):
        """Function to connect to the SQL instance"""
        try:
            logging.info("Connecting to the SQL instance")
            conn = create_engine(f"postgresql://{self.DB_USER}:{self.DB_PWD}@{self.DB_HOST}:{self.DB_PORT}/postgres")
            return conn
        except Exception as e:
            logging.error(e)
            logging.info("Failed to connect to the SQL instance")

    def load(self, data):
        """Function to load the new data to the database"""
        sql = 'select * from analyst.tbl_ustrade_time_timestamp'
        try:
            data_present = pd.read_sql(con=self.db_conn(), sql=sql)
            logging.info('DB instances')
            logging.info(data_present.columns)
            data.to_sql('tbl_macro', schema='analyst', con=self.db_conn(),
                        if_exists='append', chunksize=1000, method='multi', index=False)
            logging.info("Data loaded successfully")
        except Exception as e:
            logging.error(e)
            logging.info("Data loading failed")

    def main(self):
        logging.info('Scraping data')
        try:
            extracted_data=self.extract()
        except Exception as error_message:
            logging.info("loading file is failed. The error was: %s", error_message)
            raise RuntimeError("Scraper failed at download")
        else:
            try:
                data = self.transform(extracted_data)
            except Exception as error_message:
                logging.info("Data Transformation failed. The error was: %s", error_message)
                # raise RuntimeError("Scraper failed at dataframe transformation")
            else:
                try:
                    # pass
                    self.load(data)
                except Exception as error_message:
                    logging.info("Data load in DB failed. The error was: %s", error_message)
                    raise RuntimeError("Scraper failed at upload")




def main():
    """
    Main function executes script
    Returns:
      None
    """

    class_init = UsaTrades()
    class_init.remove_dir()
    class_init.ensure_dir()
    class_init.main()
    # print(class_init.FILE_PATH+'\\'+"Standard Report - Exports.csv")


if __name__ == "__main__":
    main()
