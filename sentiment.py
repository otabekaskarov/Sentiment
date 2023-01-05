# Necessary imports
import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup
# - install azure text analytics: pip install azure-ai-textanalytics
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential


#--------------------------------------------------------
# ETL from Amazon, Otabek Askarov.




# Step 1: Create the database and tables.

# Connect to the database
conn = sqlite3.connect('database.db')

# Create the tables
conn.execute('''
CREATE TABLE IF NOT EXISTS products (
  Product_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
''')



# Create the tables
conn.execute('''CREATE TABLE IF NOT EXISTS reviews (SID INTEGER PRIMARY KEY,
                                                             Product_id INTEGER,
                                                             User TEXT,
                                                             Date DATE,
                                                             Message TEXT,
                                                             Sentiment TEXT,
                                                             FOREIGN KEY (Product_id) REFERENCES products(Product_id));''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()


# Step 2: extracting the links from terms.txt file

links = [] # links to the products are stored in this list

with open('terms.txt', 'r') as f:
    lines = f.readlines()
    for line in lines:
        link = line.strip()
        links.append(link)



# Step 3: Scrape the product reviews, usernames, and dates.

def scrape_reviews(urlbase,numpages):
    # Initialize lists to store the scraped data
    usernames = []
    dates = []
    review_content = []

    # Set the headers for the HTTP request
    headers = {
        'User-Agent': 'Mozilla / 5.0(Windows NT10.0; Win64;x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome / 108.0.0.0 Safari / 537.36'}

    # Iterate over the pages
    for i in range(1, numpages + 1):
        # Construct the URL for the current page
        url = urlbase + str(i)

        # Send the HTTP request and get the page content
        page = requests.get(url, headers=headers)
        bs = BeautifulSoup(page.content, "html.parser")
        
        # Extract the product name from the page
        product_name = bs.find("h1", {"class": "a-size-large a-text-ellipsis"}).get_text()

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
            date_object = datetime.datetime.strptime(date_string, ' %d %B %Y')
            formatted_date = date_object.strftime('%d/%m/%Y')
            formatted_date_list.append(formatted_date)

        # Extract the review content from the page
        review = bs.find_all("span", {"data-hook": "review-body"})
        for i in range(0, len(review)):
            review_content.append(review[i].get_text())
        review_content[:] = [reviews.lstrip('\n') for reviews in review_content]
        review_content[:] = [reviews.rstrip('\n') for reviews in review_content]
        # Remove the first two elements from each list (they are not actual reviews)
        
    # Slice the lists to the same length as the smallest list
    smallest_list = len(review_content)
    usernames = usernames[:smallest_list]
    formatted_date_list = formatted_date_list[:smallest_list]
    return(product_name,usernames,formatted_date_list,review_content)

# Set the number of pages you want to scrape
num_pages = 15

for url in links:
    prod_name, Users, Dates, Messages = scrape_reviews(url,num_pages)
    
    final_list = []
    for i in range(len(Messages)):
        final_list.append((Users[i], Dates[i], Messages[i]))
        
    # Step 4: Insert the product names, product reviews, usernames, and dates into the tables created in Step 1.

    # Connect to the database
    conn = sqlite3.connect('database.db')

    # Insert a new row into the products table
    conn.execute("INSERT INTO products (name) VALUES (?)", (prod_name,))

    # Retrieve the product_id for the newly inserted row
    cursor = conn.execute("SELECT LAST_INSERT_ROWID()")

    # Get the product_id value from the cursor
    product_id = cursor.fetchone()[0]

    # Close the cursor
    cursor.close()
    
    values = [(product_id, user, date, message) for user, date, message in final_list]
    
    # Insert the values into the table
    conn.executemany('''INSERT INTO reviews (Product_id, User, Date, Message) VALUES (?, ?, ?, ?)''',values)
    
    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()




#--------------------------------------------------------

# Sentiment Analysis, Farrukh Mirzaev.

# Step 1: Cleaning the table with empty or missing values.

conn = sqlite3.connect('database.db')
cursor = conn.cursor()
cursor.execute("DELETE FROM reviews WHERE Message = '' OR Message IS NULL")
conn.commit()
conn.close()

# Step 2: Extract the messages from the database

# Connecting to the database
conn = sqlite3.connect('database.db')

# Creating a cursor
mycursor = conn.cursor()

Q2 = f"SELECT Message FROM reviews"
mycursor.execute(Q2)
lmessages = [] # messages from the database stored in this list
for i in mycursor:
    lmessages.append(i[0])
    
# Closing the connection
conn.close()


# Step 3: Get the sentiment values from Azure Text Analytics API
# - install azure text analytics: pip install azure-ai-textanalytics

# Azure Text Analytics resource key and endpoint
api_key = "155ef1b9d9144237b9a2479897342f94"
endpoint = "https://text-analytics-test1.cognitiveservices.azure.com/"

# Setting up the client
client = TextAnalyticsClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))

# Sending a request to the API to analyze the sentiment of the messages in lmessages list
# Be advised, with TextAnalytics API only 10 texts are allowed per one API request

start = 0
responses = []  # API responses are stored in responses list
while start < len(lmessages):
    end = start + 10
    response = client.analyze_sentiment(lmessages[start:end])
    responses.extend(response)
    start = end

sentiments_tuple = []  # only messages and sentiments separated from API responses are stored here as list of tuples

strenght_threshold = 0.8  # if the confidence score is higher than strenght_threshold, the sentiment is strong
for i in responses:
    message = [i.sentences[k].text for k in range(len(i.sentences))]
    message = ''.join(message)
    if i.sentiment == 'positive':
        if i.confidence_scores.values()[0] > strenght_threshold:
            sentiment = 'strong positive'
        else:
            sentiment = 'weak positive'
    elif i.sentiment == 'neutral':
        sentiment = i.sentiment
    else:
        if i.confidence_scores.values()[2] > strenght_threshold:
            sentiment = 'strong negative'
        else:
            sentiment = 'weak negative'
    
    sentiments_tuple.append((sentiment, message))


# Step 4: Insert the sentiment values into the database

# Connecting to the database
conn = sqlite3.connect('database.db')

# Creating a cursor
mycursor = conn.cursor()

# Executing the UPDATE query
mycursor.executemany("UPDATE reviews SET Sentiment = ? WHERE Message = ?", sentiments_tuple)

# Commiting the changes
conn.commit()

# Closing the connection
conn.close()

