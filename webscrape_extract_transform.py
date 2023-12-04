import snowflake.connector
import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_books(url):
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        books = []

        for book in soup.find_all('article', class_='product_pod'):
            
            title = book.h3.a.attrs['title']


            rating = book.p.attrs['class'][1]

            price = book.select('div p.price_color')[0].get_text()
            availability = book.select('div p.availability')[0].get_text().strip()

            books.append({
                'title': title,
                'rating': rating,
                'price': price,
                'availability': availability
            })

        return books
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None
    


    
"""

table creation code entered on snowflake interface 

create or replace TABLE BOOKSCRAPE.PUBLIC.BOOKS (
	TITLE VARCHAR(16777216),
	RATING NUMBER(38,0),
	PRICE FLOAT,
	AVAILABILITY BOOLEAN
);



"""

def insert_into_snowflake(data):

    # masking password.

    # uncomment the dataframe related code while running for the first time
    # if you want to save the modified and unmodified dataframes as csv
    # 1. below three lines that defines the dataframes.
    # 2. books_df and books_modified_df append 
    # 3. return line
    # 4. in main function, switch the execution line of this function.

    """columns = ['title', 'rating', 'price', 'availability']

    books_df = pd.DataFrame(columns=columns)
    books_modified_df = pd.DataFrame(columns=columns)"""


    connection = snowflake.connector.connect(
        user='BHAVYA1337',
        password='MY_PASSWORD',
        account='vo44798.ap-south-1.aws',
        warehouse='COMPUTE_WH',
        database='BOOKSCRAPE',
        schema='BOOKS'
    )

    cursor = connection.cursor()

    for book in data:
        
        #extend original dataframe
        """books_df = books_df._append({'title':book['title'], 
                                    'rating':book['rating'], 
                                    'price':book['price'], 
                                    'availability':book['availability']},ignore_index=True)"""

        # Transform rating from word to number
        rating_mapping = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
        rating = rating_mapping.get(book['rating'], None)

        # Convert price to float (assuming price is in the format '$X.XX')
        price = float(book['price'].replace('£', ''))

        # Represent availability as 0 or 1
        availability = 1 if 'in stock' in book['availability'].lower() else 0

        """books_modified_df = books_modified_df._append({'title':book['title'], 
                                                        'rating':rating, 
                                                        'price':price, 
                                                        'availability':availability},ignore_index=True)"""

        cursor.execute(
            "INSERT INTO books (title, rating, price, availability) VALUES (%s, %s, %s, %s)",
            (book['title'], rating, price, availability)
        )

    cursor.close()
    connection.commit()
    connection.close()

    #return books_df, books_modified_df

if __name__ == "__main__":
    all_books_data = []    

    # Scrape data for all pages from page-1 to page-50
    for page_number in range(1, 51):
        url = f"https://books.toscrape.com/catalogue/page-{page_number}.html"
        books_data = scrape_books(url)

        if books_data:
            all_books_data.extend(books_data)

    print(all_books_data)

    # Insert the collected data into Snowflake
    if all_books_data:
        #switch this line fi you want to save dataframes as csv in addition to steps mentioned in 
        #defination of the funtion insert_into_snowflake

        #books_df, books_modified_df = insert_into_snowflake(all_books_data)
        insert_into_snowflake(all_books_data)


    #books_df.to_csv('books.csv')
    #books_modified_df.to_csv('books_modified.csv')
