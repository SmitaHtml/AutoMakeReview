from requests import session
import json
import csv
import re
#from time import sleep
import scraper_utils
import pdb

ethasher = scraper_utils

s = session()

#Address set to prevent duplicates
addressSet = set()

spec_char = re.compile(r'[\.\,\|\n\'\"\;\-\)\(\#\/\&]')

url = "https://us.nissan-api.net/v2/dealers"

query = {
    "size":"500", # Number of returned dealerships per call
    "lat":"",
    "long":"",
    "serviceFilterType":"AND" # Required
}

# Required to get a response from the get request
headers = {
    'clientKey': "lVqTrQx76FnGUhV6AFi7iSy9aXRwLIy7",
    'apiKey': "PZUJEgLI2AwEeY3imkqxG2LOgELG3hVd"
}

with open("zips.csv", "r") as infile:
    # all zipcodes plus other location info
    latlngcsv = csv.reader(infile)
    # Grabs every 5th lat/long for each zipcode location in USA
    latlngs = [l for l in latlngcsv]
    latlngs = latlngs[::5]

    # Iterates through each line in the lat/long file
    for ll in latlngs:
        query['lat'] = ll[5] # Latitude
        query['long'] = ll[6] # Longitude
        # Try/Except used to make sure it does not error out
        # if no dealerships are returned
        try:
            response = s.get(url, params=query, headers=headers).json()
            locations = response['dealers']
            for loc in locations:
                name = loc['name']
                address = loc['address']['addressLine1']
                if "suite" in address.lower():
                    address = address.lower().split('suite')[0]
                if "po" in address.lower():
                    address = address.lower().split('po')[0]
                city = loc['address']['city']
                state = loc['address']['stateCode']
                zipcode = loc['address']['postalCode']
                fullAddress = [name, address, city, state, zipcode]
                cleanAddress = [re.sub(spec_char, "", i) for i in fullAddress]
                joinedAddress = ",".join(cleanAddress)
                addressSet.add(joinedAddress)
                print(joinedAddress)
        except Exception as e:
            print("Exception encountered: {}".format(e))
            pass

# Writing to the csv file.
out = open("nissan.csv", "w")
out.write("Name,Address,City,State,Zipcode\n")

for line in addressSet:
    out.write(line + '\n')
