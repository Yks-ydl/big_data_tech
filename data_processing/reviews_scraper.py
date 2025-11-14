from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import signal
import sys
import csv

class IMDBReviewScraper:
    def __init__(self, urls_file):
        self.file_name = urls_file
        self.service = ChromeService()
        options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(service=self.service, options=options)
        self.driver.set_page_load_timeout(10)
        self.urls = pd.read_csv(urls_file)
        self.reviews_data = pd.DataFrame(columns=['title','imdb_id', 'reviews_c','reviews_u'])
        self.wrong_urls = pd.DataFrame(columns=['title','imdb_id','url','error'])
        self.re_flile_name = self.file_name.replace('./url_data/imdb_reviews','output_imdb_reviews')
        self.reviews_data.to_csv(self.re_flile_name, index=False)
        self.wrong_urls_file_name = self.file_name.replace('./url_data/imdb_reviews','wrong_urls_imdb_reviews')
        self.wrong_urls.to_csv(self.wrong_urls_file_name, index=False)
    
    def save_r(self):
        
        save_path = self.re_flile_name
        self.reviews_data.to_csv(save_path, index=False)
    def save_w(self):
        
        save_path = self.wrong_urls_file_name
        self.wrong_urls.to_csv(save_path, index=False)
        
    def save_and_exit(self, signum, frame):
        """捕获信号后保存并退出"""
        self.close()
        save_path = self.file_name
        save_path = save_path.replace('./url_data/imdb_reviews','')
        save_path_r = f"temp_reviews{save_path}"
        save_path_w = f"temp_wrong_urls{save_path}"
        self.reviews_data.to_csv(save_path_r, index=False)
        self.wrong_urls.to_csv(save_path_w, index=False)
        print(f"\n正在处理{self.file_name}-----捕获到终止信号，已保存变量，程序退出。")
        sys.exit(0)

    def get_url_code(self,url):
        try:
            # 发送 GET 请求
            response = requests.get(url)
            
            # 检查请求是否成功（状态码 200 表示成功）
            response.raise_for_status()
            
            # 返回响应内容
            return 'success'
        
        except requests.exceptions.HTTPError as errh:
            return f"HTTP 错误: {errh}"
        except requests.exceptions.ConnectionError as errc:
            return f"连接错误: {errc}"
        except requests.exceptions.Timeout as errt:
            return f"超时错误: {errt}"
        except requests.exceptions.RequestException as err:
            return f"其他请求错误: {err}"

    def scrape_reviews_c(self):
       
        try :
            WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, "review-text")))
            
            reviews = self.driver.find_elements(By.CLASS_NAME, "review-text")
            review_texts = [review.text for review in reviews]
        except:
            review_texts = []
        return review_texts
    def scrape_reviews_u(self):
       
        # WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "audience-reviews__review js-review-text")))
            
        # reviews = self.driver.find_elements(By.CLASS_NAME, "audience-reviews__review js-review-text")
        # 用点号连接多个类名，作为CSS选择器
        
        try:
            WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, 'review-text-container')))
            rows = self.driver.find_elements(By.CLASS_NAME,'review-text-container')
            review_texts = [row.find_element(By.CLASS_NAME,'audience-reviews__review').text for row in rows]
        except:
            review_texts = []
        #review_texts = [review.text for review in reviews]
        return review_texts

    def close(self):
        self.driver.quit()
    def run(self):
        signal.signal(signal.SIGINT, self.save_and_exit)
        count_success = 0
        count_worng_urls = 0
        count_url_without_reviews = 0
        total_urls = len(self.urls)
        start_time = time.time()
        for i in range(len(self.urls)):

            url_file_upgrade = self.urls.iloc[i+1:]
            url_file_upgrade.to_csv(self.file_name, index=False)            

            print(f"Processing {self.file_name}----{i+1}/{total_urls}")
            status_code = self.get_url_code(self.urls.iloc[i]['url']) 
            if status_code != 'success':
                count_worng_urls +=1
                print (f"Wrong URL: {count_worng_urls},Successfully scraped {count_success},URL without reviews:{count_url_without_reviews}, out of {i+1} movies.")
                self.wrong_urls.loc[i] = {'title': self.urls.iloc[i]['title'], 'imdb_id': self.urls.iloc[i]['imdb_id'], 'url': self.urls.iloc[i]['url'], 'error': status_code}
                self.save_w()
                continue

            url = self.urls.iloc[i]['url']
            self.driver.get(url)
            
            title = self.urls.iloc[i]['title']
            imdb_id = self.urls.iloc[i]['imdb_id']
            
            try:
                WebDriverWait(self.driver, 1).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))).click()
            except:
                pass
            try:
                button_list = self.driver.find_element(By.CLASS_NAME,"tabs-wrap")
                buttons = button_list.find_elements(By.TAG_NAME, "rt-button")
                button_c = buttons[1]
                
                button_c.click()
                critic_reviews = self.scrape_reviews_c()
                # print(critic_reviews)
                # print (f"Scraped user reviews for {title}, url: {url}")
                button_list = self.driver.find_element(By.CLASS_NAME,"tabs-wrap")
                buttons = button_list.find_elements(By.TAG_NAME, "rt-button")
                button_u = buttons[2]
                button_u.click()
                user_reviews = self.scrape_reviews_u()
            except Exception as e:
                count_worng_urls +=1
                print (f"Wrong URL: {count_worng_urls},Successfully scraped {count_success},URL without reviews:{count_url_without_reviews}, out of {i+1} movies.")
                self.wrong_urls.loc[i] = {'title': title, 'imdb_id': imdb_id, 'url': url, 'error': str(e)}
                self.save_w()
                continue
            # print(user_reviews)
            if len(critic_reviews)+len(user_reviews) !=0:
                count_success +=1
                print (f"Wrong URL: {count_worng_urls},Successfully scraped {count_success},URL without reviews:{count_url_without_reviews}, out of {i+1} movies.")
                self.reviews_data.loc[i] = [ title,  imdb_id, critic_reviews, user_reviews]
                self.save_r()
            else:
                count_url_without_reviews +=1
                print (f"Wrong URL: {count_worng_urls},Successfully scraped {count_success},URL without reviews:{count_url_without_reviews}, out of {i+1} movies.")
        self.close()
        if os.path.exists(self.file_name):
            file_path = self.file_name
        # 删除文件
            os.remove(file_path)
        end_time = time.time()
        t = end_time - start_time
        print(f"Time taken to read URLs file: {t} seconds")
        return self.reviews_data, self.wrong_urls

if __name__ == "__main__":
        
    filenames = []
    target_dir = './url_data'  # 替换为你的目录路径
    if os.path.isdir(target_dir):
        # 获取所有文件名（不包含路径）
        filenames = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
    for i in range (len(filenames)):
        file_name = filenames[i] 
        scraper = IMDBReviewScraper(urls_file=f"{target_dir}/{file_name}")
        reviews_data, wrong_urls = scraper.run()
       