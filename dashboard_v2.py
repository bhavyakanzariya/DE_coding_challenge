import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# Snowflake connection parameters
user='BHAVYA1337'
password='sjlk23@sldOHJl'
account='vo44798.ap-south-1.aws'
warehouse='COMPUTE_WH'
database = 'BOOKSCRAPE'
schema = 'public'
table_name = 'BOOKS'

# Establish Snowflake connection
connection = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# Create a cursor
cursor = connection.cursor()

# Execute query to fetch data from Snowflake table
query = f"SELECT * FROM {table_name}"
cursor.execute(query)

# Fetch all rows from the result set
data = cursor.fetchall()

# Get column names from the cursor description
columns = [desc[0] for desc in cursor.description]

# Create a Pandas DataFrame
df = pd.DataFrame(data, columns=columns)

df.columns = df.columns.str.lower()

# Close the cursor and connection
cursor.close()
connection.close()

# Set Streamlit app title
st.title("BookScrape Bookstore")

# Sidebar with KPIs
st.sidebar.header("Summary Statistics")
total_books = len(df)
average_rating = df['rating'].mean()
total_cost = df['price'].sum()
average_cost = df['price'].mean()
availability = df['availability'].mean()*100
st.sidebar.text(f"Total Books: {total_books}")
st.sidebar.text(f"Average Rating: {average_rating:.2f}")
st.sidebar.text(f"Total Cost of Books: £{total_cost:.2f}")
st.sidebar.text(f"Average Cost of Books: £{average_cost:.2f}")
st.sidebar.text(f"Availibility : {availability}%")

# Filtered Book List
st.header("Apply Filters")
min_price, max_price = st.slider("Select Price Range", min_value=df['price'].min(), max_value=df['price'].max(), value=(df['price'].min(), df['price'].max()))# Get unique ratings from the DataFrame
# Get unique ratings from the DataFrame
# Get unique ratings from the DataFrame
all_ratings = sorted(df['rating'].unique())

# Default selection is set to all ratings
select_all_ratings = st.checkbox("Select All Ratings", value=True)

# If "Select All Ratings" is not checked, create individual checkboxes for each rating
selected_ratings = all_ratings if select_all_ratings else st.multiselect("Select Ratings", all_ratings, default=all_ratings)

# Filter DataFrame based on selected ratings
filtered_books = df[(df['price'] >= min_price) & (df['price'] <= max_price) & (df['rating'].isin(selected_ratings))]


st.dataframe(filtered_books)

# Main content area
#st.header("Price Distribution")
min_price = df['price'].min()
max_price = df['price'].max()
start_bin = int(min_price)
end_bin = int(max_price) + 1

# Create histogram
fig_price_distribution = px.histogram(df, x='price', range_x=[start_bin, end_bin],
                                      nbins=end_bin - start_bin, title='Price Distribution',
                                      histfunc='count', labels={'count': 'Number of Items'})

# Show the figure
st.plotly_chart(fig_price_distribution)

# Pie Chart of Ratings
#st.header("Ratings Distribution")
fig_ratings_pie = px.pie(df, names='rating', title='Ratings Distribution')
st.plotly_chart(fig_ratings_pie)

# Average Price by Star Rating
#st.header("Average Price by Star Rating")
avg_price_by_rating = df.groupby('rating')['price'].mean().reset_index()

# Bar Chart
fig_avg_price_by_rating = px.bar(
    avg_price_by_rating,
    x='rating',
    y='price',
    title='Average Price by Star Rating',
    labels={'price': 'Average Price', 'rating': 'Star Rating'},
)
st.plotly_chart(fig_avg_price_by_rating)
