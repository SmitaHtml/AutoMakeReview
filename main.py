import scraper_utils
import pandas as pd
import sqlite3

ethasher = scraper_utils


#ethash nissan.csv
df_nissan = pd.read_csv("nissan.csv")
#print(df_nissan)
df_nissan_ethash = pd.DataFrame() #empty dataframe
df_nissan_ethash = ethasher.ethash_locations(df_nissan, "address", "zipcode") #return dataframe with ethash
df_nissan_ethash.to_csv('nissan_ethash.csv', index=False)
print(df_nissan_ethash)

# ethash nissan_bucklocs.csv
df_nissan_bucklocs = pd.read_csv("nissan_bucklocs.csv") #input csv
#print(df_nissan_bucklocs)
df_nissan_bucklocs_ethash = pd.DataFrame() #empty dataframe
df_nissan_bucklocs_ethash = ethasher.ethash_locations(df_nissan_bucklocs, "address", "zipcode") #return dataframe with ethash
df_nissan_bucklocs_ethash.to_csv('nissan_bucklocs_ethash.csv', index=False)
print(df_nissan_bucklocs_ethash)


make = "nissan"
scrape = pd.read_csv("{}_ethash.csv".format(make))
bucklocs = pd.read_csv("{}_bucklocs_ethash.csv".format(make))

df = pd.merge(scrape, bucklocs, on=['ethash'], how="outer")

df.to_csv("{}_joined.csv".format(make), index=False)

print(df)
