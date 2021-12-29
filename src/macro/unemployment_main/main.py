import os
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import pandas as pd
import shutil
import logging
import argparse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from time import sleep
from datetime import datetime
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
    url = "https://www.bls.gov/lau/#tables"
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\\unemployment_data"
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
        x_path_2 = '//*[@id="main-content-td"]/div[4]/table[1]/tbody/tr[2]/td[6]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_2))).click()

        x_path_3 = '//*[@id="bodytext"]/form[1]/p[1]/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_3))).click()

        ActionChains(driver).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()

        x_path_4 = '//*[@id="bodytext"]/form[1]/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_4))).click()

        x_path_5 = '//*[@id="bodytext"]/form[1]/p[1]/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_5))).click()

        x_path_6 = '//*[@id="bodytext"]/form[1]/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_6))).click()

        x_path_7 = '//*[@id="bodytext"]/form[1]/p[1]/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_7))).click()
        ActionChains(driver).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()

        x_path_8 = '//*[@id="bodytext"]/form[1]/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_8))).click()

        x_path_9 = '//*[@id="bodytext"]/form[1]/p[1]/select/option[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_9))).click()

        x_path_10 = '//*[@id="bodytext"]/form[1]/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_10))).click()

        x_path_11 = '//*[@id="bodytext"]/form/p[1]/input[2]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_11))).click()

        x_path_12 = '//*[@id="bodytext"]/form/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_12))).click()

        x_path_13 = '//*[@id="bodytext"]/form/font/p[2]/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_13))).click()

        x_path_14 = '//*[@id="from-year"]/option[43]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_14))).click()

        x_path_15 = '//*[@id="bodytext"]/div[1]/form/span[1]/span[2]/input'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_15))).click()

        x_path_16 = '//*[@id="bodytext"]/div[1]/form/span[2]/span[3]/input'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_16))).click()

        x_path_17 = '//*[@id="select-output-type"]/option[3]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_17))).click()

        x_path_18 = '//*[@id="bodytext"]/form/input[1]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_18))).click()

        x_path_18 = '//*[@id="download_xlsx0"]'
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_18))).click()

        sleep(10)
        driver.quit()

    def extract(self):
        """Function to extract the data from the unzipped .csv file."""
        logging.info("Data Extraction Started")
        extract_df = pd.read_excel(self.FILE_PATH + '\\'+os.listdir(self.FILE_PATH)[0],skiprows=3)
        return extract_df

    def transform(self, data):
        logging.info('transformation begins')
        data.columns = data.columns.str.replace("[\n]", " ")
        data.drop(data.columns[[-3,-2,-1]], axis=1, inplace=True)
        df1=data.melt('Series ID', value_name='Unemploymentrate', var_name='Year')
        df= pd.read_excel(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\state and series id.xlsx')
        unemployment_rate = pd.merge(df1[['Series ID','Year','Unemploymentrate']],df[['SERIES ID','STATE']],left_on='Series ID',right_on='SERIES ID',how='left')
        unemployment_rate = unemployment_rate.drop(labels=['SERIES ID','Series ID'], axis=1)
        unemployment_rate['Year'] = pd.to_datetime(unemployment_rate['Year'].apply(lambda x: datetime.strptime(x, '%b %Y')))
        unemployment_rate['Year'] = pd.to_datetime(unemployment_rate['Year']).dt.date

        unemployment_rate.rename(columns={'STATE':'State','Unemploymentrate':'Value','Year':'Date'},inplace=True)
        df_states = pd.read_csv(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv')
        merge_df = pd.merge(unemployment_rate, df_states, on = 'State')
        final_df = merge_df
        final_df = final_df.drop(['Region', 'State Code'], axis = 1)
        final_df.rename(columns = {'Division': 'Region'}, inplace = True)
        final_df['Data Element'] = "Consumer Price Index"
        final_df['Frequency'] = "Monthly"
        final_df['Description'] = "Consumer Price Index"
        final_df = final_df[['State', 'Data Element', 'Date', 'Value', 'Frequency', 'Region', 'Description']]
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        final_df

        final_df.to_csv('population.csv',index=None)
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
                raise RuntimeError("Scraper failed at extraction")
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
