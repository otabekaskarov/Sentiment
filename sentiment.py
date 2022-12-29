import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import datetime
import smtplib
from datetime import datetime
import sqlite3
from collections import Counter
# Set the base URL for the reviews page
url_base ="https://www.amazon.de/-/en/Smartwatch-Wristband-Touchscreen-Waterproof-Stopwatch/product-reviews/B081GVYB65/ref=cm_cr_getr_d_paging_btm_prev_1?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
# Set the number of pages you want to scrape
num_pages = 15

# Initialize lists to store the scraped data
usernames = []
dates = []
review_content = []

# Set the headers for the HTTP request
headers = {
    'User-Agent': 'Mozilla / 5.0(Windows NT10.0; Win64;x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 108.0.0.0 Safari / 537.36'}

# Iterate over the pages
for i in range(1, num_pages + 1):
    # Construct the URL for the current page
    url = url_base + str(i)

    # Send the HTTP request and get the page content
    page = requests.get(url, headers=headers)
    bs = BeautifulSoup(page.content, "html.parser")

    # Extract the usernames from the page
    names = bs.find_all("span", class_="a-profile-name")
    for i in range(0, len(names)):
        usernames.append(names[i].get_text())

    # Extract the dates from the page and format them
    raw_date = bs.find_all("span", class_="review-date")
    for i in range(0, len(raw_date)):
        dates.append(raw_date[i].get_text())

    formatted_date_list = []
    for date in dates:
        date_string = date.split("on")[1]
        date_object = datetime.strptime(date_string, ' %d %B %Y')
        formatted_date = date_object.strftime('%d/%m/%Y')
        formatted_date_list.append(formatted_date)

    # Extract the review content from the page
    review = bs.find_all("span", {"data-hook": "review-body"})
    for i in range(0, len(review)):
        review_content.append(review[i].get_text())
    review_content[:] = [reviews.lstrip('\n') for reviews in review_content]
    review_content[:] = [reviews.rstrip('\n') for reviews in review_content]
    # Remove the first two elements from each list (they are not actual reviews)

# Create a Pandas DataFrame with the scraped data





"""df = pd.DataFrame()
df["Username"] = usernames
df["Dates"] = date_list
df["Review Message"] = review_content
df = df.drop_duplicates(subset=["Username", "Review Message"])



conn = sqlite3.connect("C:\\Users\\hp store\\OneDrive\\Рабочий стол\\Python_Group_Project\\reviews.db")
df.to_sql("C:\\Users\\hp store\\OneDrive\\Рабочий стол\\Python_Group_Project\\reviews.db", conn, if_exists="replace")
conn.close()"""

# Slice the lists to the same length as the smallest list

smallest_list = len(review_content)
usernames = usernames[:smallest_list]
formatted_date_list = formatted_date_list[:smallest_list]

# Connect to the database
conn = sqlite3.connect("C:\\Users\\hp store\\OneDrive\\Рабочий стол\\Python_Group_Project\\reviews.db")

# Create a cursor
cursor = conn.cursor()

# Create the table
cursor.execute('''CREATE TABLE reviews (SID INTEGER PRIMARY KEY,
                                         Product TEXT,
                                         User TEXT,
                                         Date TEXT,
                                         Message TEXT,
                                         Sentiment REAL)''')

# Commit the changes
conn.commit()
# Iterate over the lists and insert the data into the table
for i in range(len(usernames)):
    cursor.execute("INSERT INTO reviews (User, Date, Message) VALUES (?, ?, ?)", (usernames[i], formatted_date_list[i], review_content[i]))

# Commit the changes to the database
conn.commit()

# Close the connection
conn.close()

