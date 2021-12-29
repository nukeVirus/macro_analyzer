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
    url = "https://www.census.gov/housing/hvs/data/rates.html"
    FILE_PATH = os.getenv('DOWNLOAD_PATH') + "\housing_data"
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
        driver.execute_script("window.scrollTo(0, 400)")
        x_path_1="/html/body/div[1]/div[3]/div/div[1]/div[2]/div/div[1]/ul/li[3]/a"
        WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, x_path_1))).click() # 20 ms of wait time.
        sleep(15)
        driver.quit()

    def extract(self):
        """Function to extract the data from the unzipped .csv file."""
        logging.info("Data Extraction Started")
        extract_df = pd.read_excel(self.FILE_PATH + '\\tab3_state05_2021_hmr.xlsx',skiprows=3)
        return extract_df

    def transform(self, data):
        logging.info('transformation begins')
        l1 = data.columns
        l1[3].split()[-1]
        data_col_names = {}
        j=1
        for i in range(0, len(l1)):
            if j <= 7:
                data_col_names[l1[j]]=l1[j].split()[-1]+'-Q'+str(i+1)
                j += 2
        data.rename(columns=data_col_names,inplace=True)
        data=data[['State','2021-Q1','2021-Q2','2021-Q3','2021-Q4']]
        data=data.dropna(thresh=2)
        remove_dot_space = lambda x: str(x).replace('.',"").strip()
        data['State']=data['State'].map(remove_dot_space)
        data.reset_index(drop=True,inplace=True)
        split_no=data[data['State']=='State'].index
        df_1 = data.iloc[:51]
        df_2= data.iloc[51:103]
        df_3= data.iloc[103:155]
        df_2.columns = df_2.iloc[0]
        df_3.columns = df_3.iloc[0]
        l1=df_2.columns
        data_col_names_2={}
        j=1
        for i in range(0,len(l1)):
            if(i!=0):
                data_col_names_2[l1[i]]=l1[i].split()[-1]+'-Q'+str(i)
        df_2.rename(columns=data_col_names_2,inplace=True)
        l1=df_3.columns
        data_col_names_3={}
        j=1
        for i in range(0,len(l1)):
            if(i!=0):
                data_col_names_3[l1[i]]=l1[i].split()[-1]+'-Q'+str(i)
        df_3.rename(columns=data_col_names_3,inplace=True)
        df_2=df_2[1:]
        df_3=df_3[1:]
        df_1.reset_index(drop=True,inplace= True)
        df_2.reset_index(drop=True,inplace= True)
        df_3.reset_index(drop=True,inplace= True)
        df_2=df_2.rename_axis(None, axis=1)
        df_3=df_3.rename_axis(None, axis=1)
        temp_df=pd.merge(df_1,df_2,on="State",how='inner')
        absolute_df=pd.merge(temp_df,df_3,on="State",how='inner')
        melt_df1 = pd.melt(absolute_df, id_vars=['State'], var_name = 'Year', value_name = 'housing')
        melt_df1['date']=pd.to_datetime(melt_df1['Year'])
        state=pd.read_csv(r'D:\BYTEIQ\Macro Analyser USA\DOWNLOADED_DATA\states.csv')
        state=state[['State','Region']]
        process_df=pd.merge(state,melt_df1,on='State')
        process_df.rename(columns={'State':'GeoName','housing':'Value','date':'Date'},inplace=True)
        process_df['Data Element']='HOUSING'
        process_df['Frequency']='QUARTERLY'
        process_df['Description']='HOUSING DATA'
        process_df=process_df[['GeoName','Data Element','Date','Value','Frequency','Description','Region']]
        process_df.to_csv('population.csv',index=None)
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
    # class_init.remove_dir()
    # class_init.ensure_dir()
    class_init.main()
    # print(class_init.FILE_PATH+'\\'+"Standard Report - Exports.csv")


if __name__ == "__main__":
    main()
