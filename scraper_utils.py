import os
import sys
import ansi_escape_codes as terminal
import geocode
import getpass
from time import sleep
from datetime import date, datetime, timedelta
import pandas as pd
from pymongo import MongoClient


def ethash_locations(locations, address_header='address', zip_header='zipcode'):
    """
    Takes a dataframe containing and address and zipcode column and ethashes it
    :param pandas.DataFrame locations:  A df containing address info for scraped locations
    :param string address_header: Header for the address column in df (defaults to address)
    :param string zip_header: Header for the zipcode column in the df (defaults to zipcode)
    :return pandas.DataFrame locations: Returns same df with additional column, "ethash"
    """
    locations[zip_header] = locations[zip_header].astype(str)
    locations[zip_header] = locations[zip_header].apply(lambda x: x if len(x) == 5 else "0" + x)
    locations[zip_header] = locations[zip_header].apply(lambda x: x if len(x) == 5 else "0" + x)
    locations['norm_address'] = locations.apply(
        lambda row: geocode.joinClean(row[address_header], str(row[zip_header][:5])),
        axis=1
    )
    print("Geocoding Addresses")
    addresses = list(locations['norm_address'])
    add_dict = geocode.review(addresses)
    locations['ethash'] = ''
    for index, row in locations.iterrows():
        row['ethash'] = add_dict.get(row['norm_address'])
        locations['ethash'].at[index] = add_dict.get(row['norm_address'])
    locations.drop(columns=['norm_address'], inplace=True)
    return locations


def td_format(td_object):
    seconds = int(td_object.total_seconds())
    periods = [
        ('year',        60*60*24*365),
        ('month',       60*60*24*30),
        ('day',         60*60*24),
        ('hour',        60*60),
        ('minute',      60),
        ('second',      1)
    ]
    strings=[]
    for period_name, period_seconds in periods:
        if seconds > period_seconds:
            period_value , seconds = divmod(seconds, period_seconds)
            has_s = 's' if period_value > 1 else ''
            strings.append("%s %s%s" % (period_value, period_name, has_s))
    return ", ".join(strings)


def print_runstat_documentation(start_time, df):
    """
    Prints a line indended to be copied and pasted to the top of the scrape, documenting the important metrics
    Should only commit 1 log entry per PR reflective of commited state of the scrape
    :param datetime start_time: variable should be created at the very beginning of the script
    :param df: address dataframe
    :return: NA
    """
    end_time = datetime.now()
    print(terminal.red_text + "Please log the following print statement prior to submitting a pull request." + terminal.end_text_change)
    print("# Date: {4}{0}{5}     Results: {4}{1}{5}     Running Time: {4}{2}{5}     Reviewer: {4}{3}{5}".format(
        date.today(),
        len(df),
        td_format(end_time - start_time),
        getpass.getuser(),
        terminal.cyan_text,
        terminal.end_text_change
    ))

def get_address_for_latlng(lat, lng):
    """
    If the event that the best data we can scrape is just a lat lng for a locations, I've provided a function for the
    scraper to reverse geocode the lat lng to get back an address
    :param lat: str or float latitude
    :param lng: str of float longitude
    :return: {"addressLine: "", "city": "", "state": "", "zipcode": ""}
    """
    response = geocode.reverseGeocode(lat, lng)
    return response['data']['reverseGeocode'][0]


def mongo_call(env="dev", mongo_client="dev", confirm=True):
    """
    Abstracts the db call
    :param env: Development environment. Defaults to dev
    :param mongo_client: Development client. Defaults to dev
    :param confirm: Optional confirmations terminal script. Defaults to True
    :return: database cursor
    """
    if env == "prod":
        mongo_env = os.environ.get('MONGO_PROD')

    elif env == "dev":
        mongo_env = os.environ.get('MONGO_DEV')

    else:
        mongo_env = os.environ.get('MONGO_DEV')

    client = MongoClient(mongo_env)
    db = client[mongo_client]

    if confirm:
        print("\nAccessing the {2}{1}{4} client in the {3}{0}{4} development environment.\n".format(
                                                                                             env,
                                                                                             mongo_client,
                                                                                             terminal.purple_text,
                                                                                             terminal.blue_text,
                                                                                             terminal.end_text_change))
    return db


# TODO Make db collection selectable


def web_scrape_vs_db(df, value="", field="tags", env="dev", client="dev", v_test=False, preview=False, count=False):
    """
    Compares a fresh web scraped dataframe against the desired database and prints a .csv titled with the subjects
    company name, date of scrape, and focus (new, shuttered, concurrent, or non-compared) in respective
    `scraper/csv_holder/..` directory.

    `web_scrape_vs_db/8` was designed to compare based off of tags, but can be used to compare off of any field.

    It has optional terminal outputs for web scraper prototyping purposes:
    - v_test: Currently checks the query against prod/prod/bucket_locations and prod/test/business_location.
    - preview: Previews the first three lines of the dataframes for the scraped location list, Current
      business_locations, right, left, and inner comparisons of business_location query and the scraped location list.
    - count: Presents counts for the scraped location list, Current business_locations, right, left, and inner
      comparisons of business_location query and the scraped location list.

    Example:
    - <after web scraper et-hashes a scraped dataframe>
    - input: `web_scrape_vs_db(df, "APPLEBEES", "tags", "prod", "test", False, True, True)`
    - output: writes four .csv's into respective `/scrapers/csv_holder/..` directories.

    :param df: Web scraper dataframe. Required columns: name, address, city, state, zip, etHash
    :param value: Queried db value, currently optimized to be an all uppercase tag. Defaults to "".
    :param field: Queried db field. Defaults to "tags".
    :param env: Queried database environment. Defaults to "dev".
    :param client: Queried database client. Defaults to "dev".
    :param v_test: Optional Test comparison between locations ins `bucket_locations` and `business_locations`. Defaults
                   to False.
    :param preview: Optional terminal Pandas dataframe output for script prototyping purposes. Defaults to False.
    :param count: Optional terminal output for script prototyping purposes. Defaults to False.
    :return: terminal notifications and four *.csv's in the `scrapers/csv_holder/` directory.
    """

    build_query = {field: value}

    # business_locations tag query
    current_business_loc_query = mongo_call(env, client).business_locations.find(build_query)

    # Progress bar implementation (this could take a minute after all...)
    total_iterations = current_business_loc_query.count()
    iteration = 1

    current_business_loc = []

    # Iterate through json list of locations
    for loc in current_business_loc_query:
        iteration = print_progress_bar(iteration, total_iterations, '', '', 1, 13, 'WebVsDB|', terminal.red_text)
        name = loc["name"]
        address = loc["address"]
        city = loc["city"]
        state = loc["state"]
        zip = loc["zip"]
        ethash = loc["etHash"]
        # tags = loc["tags"]
        full_address = [name, address, city, state, zip, ethash]
        current_business_loc.append(full_address)

    # Load to dataframe, dedup, ethash, dump to csv, and print results
    current_business_loc_df = pd.DataFrame(current_business_loc,
                                           columns=["name", "address", "city", "state", "zipcode", "ethash"])

    # Identifies locations in latest scrape and buisiness_locations
    address_df = ethash_locations(df)
    comparison_df_inner = address_df[address_df['ethash'].isin(list(current_business_loc_df['ethash']))]

    # Identifies locations in business_locations not found in latest scrape
    merged = pd.merge(current_business_loc_df.reset_index(), address_df, on=['ethash'])
    comparison_df_right = current_business_loc_df.drop(merged['index'])

    # Identifies scraped locations not in business_locations
    merged = pd.merge(address_df.reset_index(), current_business_loc_df, on=['ethash'])
    comparison_df_left = address_df.drop(merged['index'])

    # Write .csv

    # pull company name
    company_name = df.at[1, "name"].lower()

    # grab current date
    current_time = str(time.strftime("%Y-%m-%b %d  %H:%M:%S.%Z", time.localtime()))
    scraped_time = time.strptime(current_time, "%Y-%m-%b %d  %H:%M:%S.%Z")
    scraped_date = "{}_{}_{}".format(scraped_time.tm_year, scraped_time.tm_mon, scraped_time.tm_mday)

    non_compared_location_csv = "{}_{}_non_compared_locations.csv".format(company_name, scraped_date)
    new_locations_csv = "{}_{}_new_locations.csv".format(company_name, scraped_date)
    concurrent_locations_csv = "{}_{}_concurrent_locations.csv".format(company_name, scraped_date)
    shuttered_locations_csv = "{}_{}_shuttered_locations.csv".format(company_name, scraped_date)

    # Finds the writing directory
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    non_compared_csv_path = os.path.join(script_dir,
                                    "../../../../../../../csv_holder/non_compared_locations/{}".format(
                                        non_compared_location_csv))
    new_loc_csv_path = os.path.join(script_dir,
                                    "../../../../../../../csv_holder/new_locations/{}".format(
                                        new_locations_csv))
    concurrent_loc_csv_path = os.path.join(script_dir,
                                           "../../../../../../../csv_holder/concurrent_locations/{}".format(
                                               concurrent_locations_csv))
    shuttered_loc_csv_path = os.path.join(script_dir,
                            "../../../../../../../csv_holder/shuttered_locations/{}".format(
                                shuttered_locations_csv))

    # Writes the discrete .csv's with the concatenated company name and scraping date
    comparison_df_inner.to_csv(non_compared_csv_path, index=False)
    comparison_df_left.to_csv(new_loc_csv_path, index=False)
    current_business_loc_df.to_csv(concurrent_loc_csv_path, index=False)
    comparison_df_right.to_csv(shuttered_loc_csv_path, index=False)

    # Optional terminal outputs
    # counts
    if count:
        updated_location_count = len(df)
        current_business_loc_count = len(current_business_loc)
        comparison_count = len(comparison_df_inner)
        right_count = len(comparison_df_right)
        left_count = len(comparison_df_left)

        # add proper titles
        print(
            "\nNew web scraper Locations: \t\t{2}{0}{3}"
            "\nCurrent Business Locations: \t\t{2}{1}{3}"
            "\nOverlapping location count: \t\t{2}{4}{3}"
            "\nLocation only in .csv \t\t\t{2}{6}{3}"
            "\nLocation only in business_locations \t{2}{5}{3}\n".format(
                                            updated_location_count,
                                            current_business_loc_count,
                                            terminal.cyan_text,
                                            terminal.end_text_change,
                                            comparison_count,
                                            right_count,
                                            left_count
                                            )
                                        )
    # bucket_location query
    if v_test:
        base_db_query = mongo_call("prod", "prod", False).bucket_locations.find(build_query).count()
        test_query = mongo_call("prod", "test", False).business_locations.find(build_query).count()

        print("\n{0}prod{2}/{0}prod{2} bucket_locations count: \t{0}{1:6}{2}\n"
              "{6}prod{2}/{6}test{2} business_locations count: \t{6}{5:6}{2}\n".format(
                                                                                terminal.red_text,
                                                                                base_db_query,
                                                                                terminal.end_text_change,
                                                                                env,
                                                                                client,
                                                                                test_query,
                                                                                terminal.blue_text))

    # Terminal Preview of .csv output (default is False)
    if preview:
        print("\n{1}{0}{3} New web scraper Locations: \n{1}{2}{3}".format(len(address_df),
                                                             terminal.blue_text,
                                                             address_df.head(3),
                                                             terminal.end_text_change))
        print("\n{1}{0}{3} Current Business Locations: \n{1}{2}{3}".format(len(current_business_loc_df),
                                                            terminal.red_text,
                                                            current_business_loc_df.head(3),
                                                            terminal.end_text_change))
        print("\n{1}{0}{3} Inner comparison (Overlapping .csv and business_location locations): \n{1}{2}{3}".format(len(comparison_df_inner),
                                                                                                       terminal.purple_text,
                                                                                                       comparison_df_inner.head(3),
                                                                                                       terminal.end_text_change))
        print("\n{1}{0}{3} Left comparison (Location only in .csv): \n{1}{2}{3}\n".format(len(comparison_df_left),
                                                                           terminal.green_text,
                                                                           comparison_df_left.head(3),
                                                                           terminal.end_text_change))
        print("\n{1}{0}{3} Right comparison (Location only in business_locations): \n{1}{2}{3}".format(len(comparison_df_right),
                                                                                          terminal.white_text,
                                                                                          comparison_df_right.head(3),
                                                                                          terminal.end_text_change))
