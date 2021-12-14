from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime


URL = "https://www.bls.gov/charts/consumer-price-index/consumer-price-index-by-region.htm"
FILE_PATH ="D:\\BYTEIQ\\macro_test"
WEBDRIVER_PATH = "C:\\Users\\LENOVO\\Desktop\\demo\\chromedriver.exe"

chrome_options = webdriver.ChromeOptions()
chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": FILE_PATH}
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(executable_path = WEBDRIVER_PATH, options=chrome_options)
print("chrome opened")
driver.get(URL)
x_path = '/html/body/div[2]/div/div/div[5]/div/div[3]/div[2]/a'
WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, x_path))).click()
html_source = driver.page_source
# print(html_source)
driver.quit()

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
final_df.to_excel("D:\\BYTEIQ\\macro_test\\"+'consumer_price_index_%removed.xlsx', index = None)

