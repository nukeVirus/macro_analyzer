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
    url = "https://apps.bea.gov/itable/iTable.cfm?ReqID=70&step=1&acrdn=1#"
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\pcpi_data"
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
        driver.maximize_window()
        logging.info("chrome opened")
        driver.get(self.url)
        x_path_1 = '//*[@id="vertical_container_1"]/div[2]/div[1]/a'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_1))).click() # 20 ms of wait time.

        x_path_2 = '//*[@id="tabpanel_22_336_1_0_70"]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_2))).click() # 20 ms of wait time.

        x_path_3 = '//*[@id="myform7"]/div[1]/div/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_3))).click() # 20 ms of wait time.

        x_path_4 = '//*[@id="myform7"]/div[2]/div/select/option[4]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_4))).click() # 20 ms of wait time.

        x_path_5 = '//*[@id="goto7"]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_5))).click() # 20 ms of wait time.

        x_path_6 = '/html/body/div[1]/div[2]/div/div/div[2]/div/div[3]/div/div[3]/form/div[1]/div/select/option[4]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_6))).click() # 20 ms of wait time.

        x_path_6 = '/html/body/div[1]/div[2]/div/div/div[2]/div/div[3]/div/div[3]/form/div[1]/div/select/option[3]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_6))).click() # 20 ms of wait time.

        x_path_7 = '//*[@id="goto8"]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_7))).click() # 20 ms of wait time.

        x_path_10 = '//*[@id="showDownload"]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_10))).click() # 20 ms of wait time.
        x_path_11 = '//*[@id="download_wraper"]/div/a[2]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_11))).click()
        time.sleep(20)
        driver.quit()
        logging.info('colsed')

    def extract(self):
        """Function to extract the data from the unzipped .csv file."""
        logging.info("Data Extraction Started")
        extract_df = pd.read_csv(self.FILE_PATH +'\\download.csv',skiprows=4).dropna()
        return extract_df

    def transform(self, process_df):
        remove_asterik = lambda data : data.split('*')[0].strip()
        process_df['GeoName'] = process_df['GeoName'].map(remove_asterik)
        process_df = process_df [process_df['GeoName'] != 'United States']
        process_df = process_df.drop(['GeoFips'], axis=1)
        process_df.drop('LineCode',axis=1,inplace=True)
        melt_df = process_df
        melt_df = pd.melt(melt_df, id_vars=['GeoName','Description'], var_name= 'Quarter', value_name = "Value")
        remove_colon = lambda data : data.strip().replace(":", '')
        melt_df['Quarter'] = melt_df['Quarter'].map(remove_colon)
        melt_df['Date'] = pd.to_datetime(melt_df['Quarter'])
        grouped_df = melt_df.groupby(by=["GeoName"])
        df_3 = pd.DataFrame()
        for key, item in grouped_df:
            df_3 = df_3.append(item)
        df_3.reset_index(drop = True)
        df_3 = df_3.rename(columns = {'GeoName': 'State'})
        state_df = pd.read_csv(r"D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv")
        state_df = state_df[['State', 'Region']]
        final_df = pd.merge(df_3, state_df, on= 'State')
        # final_df['Description'] = "Per Capita Personal Income (in dollars)"
        final_df['Frequency'] = 'Quaterly'
        final_df = final_df.rename(columns = {'State':'GeoName'})
        final_df['Data Element'] = "Per Capita Personal Income"
        col_name={'GeoName':'State'}
        final_df.rename(columns=col_name,inplace=True)
        final_df = final_df[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Description', 'Region']]
        final_df.to_csv(self.FILE_PATH+'\\pcpi.csv')
        

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
            # pass
            self.download()
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
    class_init.remove_dir()
    class_init.ensure_dir()
    class_init.main()
    # print(class_init.FILE_PATH+'\\'+"Standard Report - Exports.csv")


if __name__ == "__main__":
    main()
