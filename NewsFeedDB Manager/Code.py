import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
import urllib.request
import xml.etree.ElementTree as ET #Module I learned from Youtube to work with the XML directly
from pyspark.sql import Row
import feedparser

appName = "First Data analaysis"
master = "local"
conf = pyspark.SparkConf() \
    .set('spark.driver.host', '*****') \ ##"****"# had my ip address as the host ip address 
    .setAppName(appName) \
    .setMaster(master)

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession.builder.getOrCreate()

rss_url = 'https://news.google.com/rss/search?q=technology&hl=en-US&gl=US&ceid=US:en' ###The URL i used to feed the news.

response = urllib.request.urlopen(rss_url)
rss_data = response.read()
root = ET.fromstring(rss_data)
data = []
for item in root.findall('./channel/item'):
    title = item.find('title').text if item.find('title') is not None else None
    link = item.find('link').text if item.find('link') is not None else None
    pubDate = item.find('pubDate').text if item.find('pubDate') is not None else None
    description = item.find('description').text if item.find('description') is not None else None
    source = item.find('source').text if item.find('source') is not None else None
    data.append(Row(title=title, link=link, pubDate=pubDate, description=description, source=source))

news_df = spark.createDataFrame(data)

postgres_url = "jdbc:postgresql://localhost:5432/postgres"
postgres_properties = {
    "user": "*****", ##"****"# had my username credentials  
    "password": "****", ##"****"# had my password credentials  
    "driver": "org.postgresql.Driver"
}
news_df.write.jdbc(url=postgres_url, table='news.q1', mode='append', properties=postgres_properties)
news_df.show(5)

def get_recent_news(spark, rss_url, hours=24): ## Function in 3. of the ReadME file of this directory.
    feed = feedparser.parse(rss_url)
    data = []
    for entry in feed.entries:
        title = entry.title if 'title' in entry else None
        link = entry.link if 'link' in entry else None
        pubDate = entry.published if 'published' in entry else None
        description = entry.description if 'description' in entry else None
        source = entry.source.title if 'source' in entry else None
        data.append(Row(title=title, link=link, pubDate=pubDate, description=description, source=source))
    news_df = spark.createDataFrame(data)
    news_df = news_df.withColumn(
        "pubDate",
        F.to_timestamp(F.col("pubDate"), "E, dd MMM yyyy HH:mm:ss Z")
    )
  
    current_time = F.current_timestamp()
    recent_news_df = news_df.filter((current_time - F.col("pubDate")) <= F.expr(f'INTERVAL {hours} HOURS'))
    return recent_news_df
    recent_news_df = get_recent_news(spark, rss_url, hours=24)
    recent_news_df.show()
    recent_news_df.write.jdbc(url=postgres_url, table='news.q1', mode='append', properties=postgres_properties) ## To populate this to my table news on my local postgres PGadmin4

def fetch_rss_data(url, category): 
    response = requests.get(url)
    root = ET.fromstring(response.content)
    data = []
    for item in root.findall('.//item'):
        title = item.find('title').text if item.find('title') is not None else None
        link = item.find('link').text if item.find('link') is not None else None
        pubDate = item.find('pubDate').text if item.find('pubDate') is not None else None
        description = item.find('description').text if item.find('description') is not None else None
        data.append(Row(title=title, link=link, pubDate=pubDate, description=description, category=category))
    return data

all_data = []
for category, url in feeds.items():
    all_data.extend(fetch_rss_data(url, category))
news_df = spark.createDataFrame(all_data)
news_df = news_df.withColumn("pubDate", F.to_timestamp(F.col("pubDate"), "E, dd MMM yyyy HH:mm:ss Z")) 
news_df.write.jdbc(url=postgres_url, table='news.q1', mode='append', properties=postgres_properties)
distinct_categories_df = news_df.select('category').distinct()
distinct_categories_df.show()

feeds = {'technology': 'https://news.google.com/rss/search?q=technology&hl=en-US&gl=US&ceid=US:en','business': 'https://news.google.com/rss/search?q=business&hl=en-US&gl=US&ceid=US:en',
    'sports': 'https://news.google.com/rss/search?q=sports&hl=en-US&gl=US&ceid=US:en'}
distinct_categories_df = news_df.select('category').distinct()
def fetch_rss_data(url, category):
    response = requests.get(url)
    root = ET.fromstring(response.content)
    data = []
    for item in root.findall('.//item'):
        title = item.find('title').text if item.find('title') is not None else None
        link = item.find('link').text if item.find('link') is not None else None
        pubDate = item.find('pubDate').text if item.find('pubDate') is not None else None
        description = item.find('description').text if item.find('description') is not None else None
        data.append(Row(title=title, link=link, pubDate=pubDate, description=description, category=category))
    return data
all_data = []
for category, url in feeds.items():
    all_data.extend(fetch_rss_data(url, category))
  
news_df = spark.createDataFrame(all_data)
news_df = news_df.withColumn(
    "pubDate",
    F.to_timestamp(F.col("pubDate"), "E, dd MMM yyyy HH:mm:ss Z")
)

news_df.write.jdbc(url=postgres_url, table='news.q1', mode='append', properties=postgres_properties)
def delete_nfl_records():
    delete_query = """
    DELETE FROM news.q1
    WHERE title LIKE '%NFL%';
    """
    conn = None
    stmt = None
    try:
        conn = spark._jvm.java.sql.DriverManager.getConnection(postgres_url, "postgres", "Sakhaleyash@1")
        stmt = conn.createStatement()
        stmt.executeUpdate(delete_query)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()
delete_nfl_records()
distinct_categories_df = spark.read.jdbc(url=postgres_url, table='news.q1', properties=postgres_properties).select('category').distinct()
