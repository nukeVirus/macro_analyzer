import os
import time
from datetime import date
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
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/state/totals/nst-est2020.csv'
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\population_data"
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
        chromeOptions = webdriver.ChromeOptions()
        prefs = {"download.default_directory": self.FILE_PATH}
        chromeOptions.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path=self.PATH_TO_CHROME,
                                  options=chromeOptions)
        logging.info("chrome opened")
        driver.get(self.url)
        time.sleep(15)
        driver.quit()

    def extract(self):
        """Function to extract the data from the unzipped .csv file."""
        logging.info("Data Extraction Started")
        extract_df = pd.read_csv(self.FILE_PATH + '\\nst-est2020.csv')
        return extract_df

    def transform(self, process_df):
        logging.info('transformation begins')
        
        process_df.loc[0:4]
        process_df = process_df.drop(process_df.index[0:5])
        process_df = process_df.reset_index(drop = True)
        df_2 = process_df[['NAME', 'POPESTIMATE2015', 'POPESTIMATE2016', 'POPESTIMATE2017', 'POPESTIMATE2018', 'POPESTIMATE2019', 'POPESTIMATE2020']]
        df_2 = pd.melt(df_2, id_vars=['NAME'], var_name='Date', value_name='Value')
        remove_popestimate = lambda data : data.replace('POPESTIMATE', '')
        df_2['Date'] = df_2['Date'].map(remove_popestimate)
        df_2['Date'] = pd.to_datetime(df_2['Date'])
        df_2.rename(columns = {'NAME':'State'}, inplace = True)
        df_states = pd.read_csv(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv')
        df_states = df_states[['State', 'Region']]
        df_merge = pd.merge(df_2, df_states, on = 'State')
        df_merge['Frequency'] = 'Yearly'
        df_merge['Description'] = 'POPULATION'
        df_merge['Data Element'] = 'POPULATION'
        df_merge.rename(columns = {'GeoName':'State'}, inplace = True)
        df_final = df_merge[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Region', 'Description']]


        return df_final


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
