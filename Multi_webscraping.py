# Import packages
from selenium import webdriver
from datetime import datetime
import time
import pandas as pd
import os
import io
import requests
import argparse
import socket
import itertools
import ipaddress
from urllib.parse import urlparse
from multiprocessing import Pool, cpu_count

# Define the Chrome options to open the window in incognito mode
option = webdriver.ChromeOptions()
option.add_argument('--incognito')
option.add_argument('--ignore-certificate-errors-spki-list')
option.add_argument('--ignore-ssl-errors')

# Find the ChromeDriver path
SNS_dir = os.path.abspath(os.curdir)
Scrap_dir = os.path.join(SNS_dir, 'Scraping')
DRIVER_PATH = os.path.join(Scrap_dir, 'chromedriver')

# Retrieve images url function
def fetch_img_urls(query, country , img_to_fetch, wd):
    def scroll_to_end(wd):
        wd.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    # Build a search query for picture more than 4 Megapixels
    search_url = 'https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&cr=country{c}&gs_l=img&tbs=isz:lt,islt:8mp'
    wd.get(search_url.format(q=query, c=country))
    
    img_urls = set()
    img_count = 0
    results_start = 0
    
    while img_count < img_to_fetch:
        scroll_to_end(wd)
        
        # Get all the image thumbnail results
        thumbnail_results = wd.find_elements_by_css_selector("img.Q4LuWd")
        num_results = len(thumbnail_results)
        
        print(f"Found: {num_results} search results. Extracting links from {results_start}:{num_results}")
        
        for img in thumbnail_results[results_start:num_results]:
            # Clicking the resulting thumbnail to get real image 
            try:
                img.click()
                time.sleep(0.5)
            except Exception:
                continue

            # Extract the image urls and get rid of the encrypted gstatic links
            real_img = wd.find_elements_by_css_selector('img.n3VNCb')
            for x in real_img:
                if x.get_attribute('src') and 'http' in x.get_attribute('src') and not 'gstatic' in x.get_attribute('src'):
                    img_urls.add(x.get_attribute('src'))
    
            # Total number of images urls extracted
            img_count = len(img_urls)
            
            # Set the limit for retrieved image urls
            if img_count >= img_to_fetch:
                print(f"Image links found: {img_count} ... DONE!!!")
                break
        else:
            print("Looking for more image links ...")
            time.sleep(30)
            return
            show_more_results = wd.find_element_by_css_selector(".mye4qd")
            if show_more_results:
                wd.execute_script("document.querySelector('.mye4qd').click();")

        # move the result startpoint further down
        results_start = len(thumbnail_results)

    return img_urls

# Get the IP address of image urls
def get_ipaddr(img_urls):
    # Display the result in dataframe
    url_list = []
    ip_list = []
    
    # Break the URL into component and get the IP address of URL
    for urls in img_urls:
        website = urlparse(urls)
        ip_addr = socket.gethostbyname(website.netloc)
        print(f"URL: {website.netloc} and IP: {ip_addr}")
        url_list.append(website.netloc)
        ip_list.append(ip_addr)
    
    url_ip_dict = dict(zip(url_list, ip_list))
    dataframe = pd.DataFrame([[keys, values] for keys, values in url_ip_dict.items()]).rename(columns={0:'URL', 1:'IP address'})
    
    return dataframe, ip_list

# Calculating throughput of each URLs
def calc_througput(ssthresh, img_urls):
    data = 0
    throughput = 0
    throughput_list = []
    
    # Calculating throughput and set timeout for very large files
    for urls in img_urls:
        try:
            start_time = time.time()
            img_content = len(requests.get(urls, stream=True).content)
            if img_content > ssthresh:
                data += img_content
        except Exception as e:
            print(f"ERROR - Could not download {urls} - {e}")
            data = 0

        end_time = time.time()
        throughput = data/(end_time - start_time)
        throughput_list.append(throughput)
    
    return throughput_list

# Convert IP address into 32 dimension input space 
def convert2bin(ip_list):
    bin_ip = []
    
    for ip in ip_list:
        ip_vector = format(int(ipaddress.ip_address(ip)), '032b')
        bin_ip.append(ip_vector)
        
    return bin_ip

# Pipeline of the image scraping function
def pipeline(country):
    # Argument is tuple of (Continent, [list of countries])
    # Retrieve image urls
    with webdriver.Chrome(executable_path=DRIVER_PATH, options=option) as wd:
        start_timer = datetime.now()
        img_urls = fetch_img_urls('food', country, 50, wd)
        time_elapsed = datetime.now() - start_timer
        print("Time elapsed (hh:mm:ss.ms) {}".format(time_elapsed))
    
        # Get unique IP address of each images urls and convert to binary 32 bits
        df, ip_addr = get_ipaddr(img_urls)
        ip_binary = convert2bin(ip_addr)

        # Create a duplicated countries list to map the IP address and throughput
        cr_list = [country] * len(img_urls)

        # Calculating throughput
        throughput = calc_througput(65535, img_urls)
        dataframe = pd.DataFrame(list(zip(cr_list, ip_addr, throughput, ip_binary)), \
                                 columns=['Country', 'IP address', 'Throughput', 'Binary IP'])

        # Splitting binary 32 bits IP address into multidimensional columns for machine learning
        for i in range(32):
            dataframe['B'+str(i)] = dataframe['Binary IP'].str[i]
    
    return dataframe

if __name__ == '__main__':
    # Multithreaded to fetch image by parsing different countries
    country_dict = {'Asia': ['CN', 'IN', 'JP', 'KR', 'TW'],
                    'Europe': ['GB', 'FR', 'IT', 'DE', 'RU'],
                    'Africa': ['NG', 'EG', 'ET', 'TZ', 'ZA'],
                    'Oceania': ['NZ', 'AU'],
                    'South America': ['MX', 'CO', 'AR', 'BR', 'CL'],
                    'North America': ['US', 'CA']}

    # Convert dictionary values to list for multiprocessing pool
    country_list = [(values) for values in country_dict.values()]
    merge = list(itertools.chain(*country_list))
    print(merge)

    # max number of parallel process
    with Pool(processes=4) as pool:
        results = pool.map(pipeline, merge)

    result_df = pd.concat(results)
    print(result_df)

    result_df.to_csv('dataset.csv', index=False)