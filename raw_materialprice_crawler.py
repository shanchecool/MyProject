from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
# import data_pipeline_to_hive

# chrome_path = 'D://work/chromedriver.exe'
chrome_path = '/home/oracle/chromedriver'
stock_map = {'Aluminum': 'dj-aluminum'}



# 測試參數
stock_name = 'Aluminum'
start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = start_date

# start to crawl
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(executable_path=chrome_path,
                          chrome_options=chrome_options)
driver.get('https://www.investing.com/indices/' + stock_map[
    stock_name] + '-historical-data')
driver.implicitly_wait(10)
flag = 0






try:
    # time.sleep(20)
    # popup = driver.find_elements_by_css_selector(
    #     '#PromoteSignUpPopUp > div.right > i')
    # if popup:
    #     popup[0].click()

    button = driver.find_element_by_xpath('//*[@id="widgetFieldDateRange"]')
    button.click()
    time.sleep(2)
    startDate_input = driver.find_element_by_xpath('//*[@id="startDate"]')
    startDate_input.clear()
    startDate_input.send_keys(
        datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%Y'))
    time.sleep(2)
    dndDate_input = driver.find_element_by_xpath('//*[@id="endDate"]')
    dndDate_input.clear()
    dndDate_input.send_keys(
        datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y'))
    time.sleep(2)
    apply = driver.find_element_by_xpath('//*[@id="applyBtn"]')
    apply.send_keys(Keys.ENTER)
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('table',
                      attrs={'class': 'genTbl closedTbl historicalTbl'})
    table_body = table.find('tbody')

    if table_body.find('tr').find('td').text == 'No results found':
        flag = 1

    onedate = []
    price = []
    openp = []
    highp = []
    lowp = []
    volume = []
    if flag == 0:
        for tr in table_body.find_all('tr'):
            onedate.append(
                datetime.strptime(tr.find_all('td')[0].text, '%b %d, %Y'))
            price.append(float(tr.find_all('td')[1].text))
            openp.append(float(tr.find_all('td')[2].text))
            highp.append(float(tr.find_all('td')[3].text))
            lowp.append(float(tr.find_all('td')[4].text))

            if 'M' in tr.find_all('td')[5].text:
                volume.append(
                    round(float(tr.find_all('td')[5].text.replace('M', '')) * 1000000,
                          1))
            elif 'K' in tr.find_all('td')[5].text:
                volume.append(
                    round(float(tr.find_all('td')[5].text.replace('K', '')) * 1000, 1))
            else:
                volume.append(float(tr.find_all('td')[5].text))
        stockdf = pd.DataFrame({'stockname': stock_map[stock_name],
                                'stockdate': onedate, 'price': price,
                                'open': openp, 'high': highp,
                                'low': lowp, 'volume': volume})
        # print(stockdf)
        stockdf.to_csv("/home/oracle/turnover/stockprice/stockdf_" + datetime.now().strftime("%Y-%m-%d") + ".csv",
                       index= False)
        # data_pipeline_to_hive.pipeline(stockdf)
    else:
        print("no result")

finally:
    driver.quit()

# datetime.today() equals datetime.now(tz=None)
