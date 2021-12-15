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
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\gdp_data"
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
        x_path_1 = '//*[@id="tabpanel_22_5532_1_0_70"]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_1))).click() # 20 ms of wait time.
        x_path_2 = '/html/body/div[1]/div[2]/div/div/div[2]/div/div[3]/div/div[2]/form/div[1]/div/select/option[1]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_2))).click() # 20 ms of wait time.
        x_path_3 = '/html/body/div[1]/div[2]/div/div/div[2]/div/div[3]/div/div[2]/form/div[2]/div/select/option[2]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_3))).click() # 20 ms of wait time.

        x_path_4 = '//*[@id="goto7"]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_4))).click() # 20 ms of wait time.

        x_path_5 = '//*[@id="myform8"]/div[1]/div/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_5))).click() # 20 ms of wait time.

        x_path_12='/html/body/div[1]/div[2]/div/div/div[2]/div/div[3]/div/div[3]/form/div[1]/div/select/option[2]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_12))).click()

        x_path_8 ="/html/body[@class='apps-bea-gov path-dummynotfoundhtm navbar-is-fixed-top has-glyphicons']/div[@id='main-content']/div[@class='row']/div[@class='col-sm-12 app-itables']/div[@class='region region-content']/div[@id='wraper']/div[@id='xmlWraper']/div[@id='geno']/div[@class='tab-content']/div[@id='panel-8']/form[@id='myform8']/div[@class='form-group row'][4]/div[@class='col-sm-10']/span[@id='goto8']"
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path_8))).click() # 20 ms of wait time.
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
        extract_df = pd.read_csv(self.FILE_PATH + '/' + 'download.csv', skiprows=4)
        return extract_df

    def transform(self, data):
        logging.info('transformation begins')
        todays_date = date.today()
        column_name=[]
        for i in data.columns[1:4]:
            column_name.append(i)
        for i in data.columns[5:]:
            if int(i.split(':')[0])>todays_date.year-3:
                column_name.append(i)
        data=data[column_name]
        data=data.dropna().drop('LineCode',axis=1)
        data=data[data['GeoName']!='United States']
        pd.set_option('display.max_rows', None)
        data.reset_index(drop=True,inplace=True)
        state=pd.read_csv(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv')
        state.rename(columns={'State':'GeoName'},inplace=True)
        state=state[['GeoName','Region']]
        process_df=pd.melt(data, id_vars=['GeoName','Description'], var_name='Year-Month', value_name='GDP')
        process_df=pd.merge(process_df,state,on='GeoName')
        process_df['Data Element']='GDP'
        process_df['Frequency']='Quarterly'
        remove_colon= lambda data:data.replace(':',"").strip()
        process_df['Year-Month']=process_df['Year-Month'].map(remove_colon)
        process_df['Year-Month']=pd.to_datetime(process_df['Year-Month'])
        col_name={'GeoName':'State','Year-Month':'Date','GDP':'Value'}
        process_df.rename(columns=col_name,inplace=True)
        process_df = process_df[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Region', 'Description']]
        print(process_df.columns)
        # process_df.to_csv(self.FILE_PATH+'\gdp_data.csv')
        # logging.info('transformation ends')

        return process_df


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
