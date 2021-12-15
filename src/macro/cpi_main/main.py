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
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC



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
    URL = "https://www.bls.gov/charts/consumer-price-index/consumer-price-index-by-region.htm"
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
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        prefs = {"download.default_directory": self.FILE_PATH}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path = self.PATH_TO_CHROME, options=chrome_options)
        logging.info("Extraction Started")
        driver.get(self.URL)
        x_path = '/html/body/div[2]/div/div/div[5]/div/div[3]/div[2]/a'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path))).click()
        html_source = driver.page_source
        driver.quit()
        return html_source

    def transform(self, html_source):
        logging.info('transformation begins')
        df_1 = pd.read_html(html_source)[0]
        df_1['Month'] = pd.to_datetime(df_1['Month'])
        df_1['Month'] = (df_1['Month']).astype(str)
        df_2 = df_1[df_1['Month'].str.split('-').str[0].astype(int)> datetime.now().year - 3]
        df_2 = df_2.reset_index(drop = True)
        melt_df = pd.melt(df_2, id_vars = ['Month'], var_name = 'Division', value_name = 'Value')
        melt_df.rename(columns={'Month': 'Date'}, inplace = True)
        df_states = pd.read_csv(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv')
        merge_df = pd.merge(melt_df, df_states, on = 'Division')
        final_df = merge_df
        final_df = final_df.drop(['Region', 'State Code'], axis = 1)
        final_df.rename(columns = {'Division': 'Region'}, inplace = True)
        final_df['Data Element'] = "Consumer Price Index"
        final_df['Frequency'] = "Monthly"
        final_df['Description'] = "Consumer Price Index"
        final_df = final_df[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Region', 'Description']]
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        remove_modulous = lambda data : data.replace('%', '')
        final_df['Value'] = final_df['Value'].map(remove_modulous)
        final_df['Value'] = final_df['Value'].astype(float)
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
        try:
            logging.info('DB instances')
            data.to_sql('tbl_macro', schema='analyst', con=self.db_conn(),
                        if_exists='append', chunksize=1000, method='multi', index=False)
            logging.info("Data loaded successfully")
        except Exception as e:
            logging.error(e)
            logging.info("Data loading failed")

    def main(self):
        logging.info('Scraping data')
        try:
            pass
            # self.download()
        except Exception as error_message:
            logging.info("Downloading the file is failed. The error was: %s", error_message)
            raise RuntimeError("Scraper failed at download")
        else:
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
    # class_init.remove_dir()
    # class_init.ensure_dir()
    class_init.main()
    # print(class_init.FILE_PATH+'\\'+"Standard Report - Exports.csv")


if __name__ == "__main__":
    main()
