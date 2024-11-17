try:
    import re
    import os
    import time
    import string
    import logging # Optional: for logging purposes
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
    from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException
    from bs4 import BeautifulSoup
    from datetime import datetime
    import pandas as pd
except ImportError as e:
    print(f"Error: {e}")
    print("Please install the required packages using the following command:")
    print("pip install selenium pandas beautifulsoup4")
    exit(1)
    
if __name__ == "__main__":
    
    # Path to Chromedriver
    PATH = "/Users/saggysimmba/Downloads/chromedriver-mac-arm64/chromedriver"
    
    # Path to Downloads 
    download_folder = "/Applications/saggydev/Fall 2024/CSE 512/bonus/data_downloads"
    
      # Headless mode 
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox') # Bypass OS security
    chrome_options.add_argument('--disable-dev-shm-usage') # Overcome limited resource problems
    
    
    # Specify the download directory
    prefs = {
    'download.default_directory': download_folder,
    'download.prompt_for_download': False,  # To auto-confirm downloads
    'download.directory_upgrade': True,
    'safebrowsing_for_trusted_sources_enabled': False,  # To disable security warnings
    'safebrowsing.enabled': False
    }
    
    chrome_options.add_experimental_option('prefs', prefs)

    # Creating a service object using the ChromeDriver executable path
    service = Service(PATH)
    print(service)
    
    