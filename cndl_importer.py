#!/usr/bin/env python3

import datetime
import cbpro
import time
import pytz
import pandas as pd
import tqdm
import math
import scipy

public_client = cbpro.PublicClient()


def menu_structure(prompt, *args):
    choices = []
    print(prompt)
    for c in args:
        if isinstance(c, list):
            for x in c:
                choices.append(x)
        else:
            choices.append(c)

    for e, c in enumerate(choices):
        print(e, ":", c)
    while True:
        try:
            choice = choices[int(input('Enter a choice:'))]
            print(choice)
            break
        except Exception:
            print('Invalid Entry. Try again.')
    return choice


def get_non_negative_int(prompt):
    while True:
        try:
            value = int(input(prompt))
        except ValueError:
            print("Sorry, please try again...")
            continue
        if value < 0:
            print("Must be a positive number. Please try again.")
            continue
        else:
            break
    return value


def get_products():
    products = public_client.get_products()
    print("GDAX Available Products:")
    for index, product in enumerate(products):
        print(index, product['id'])
    choice = get_non_negative_int('Please Select a product: ')
    print(products[choice]['id'])
    return products[choice]['id']


def import_new():
    print("Please select a product to import.")
    product = get_products()

    candle_sizer = menu_structure("Enter candle size for {}.".format(product), "1 Day", "6 Hour", "1 Hour", "15 Minute",
                                  "5 Minute", "1 Minute")

    if candle_sizer == "1 Day":
        candle_size = 86400
    elif candle_sizer == "6 Hour":
        candle_size = 21600
    elif candle_sizer == "1 Hour":
        candle_size = 3600
    elif candle_sizer == "15 Minute":
        candle_size = 900
    elif candle_sizer == "5 Minute":
        candle_size = 300
    elif candle_sizer == "1 Minute":
        candle_size = 60

    while True:
        query = "Please enter a START DATE in the following format [ MM/DD/YY HH:MM ] e.g. 01/01/18 23:00 \n" \
                "All requests should be entered in UTC & 24hr notation\n" \
                "Start:"

        try:
            start_time = datetime.datetime.strptime(input(query), "%m/%d/%y %H:%M")
            start_time = start_time.replace(tzinfo=pytz.timezone('UTC'))
            print(start_time)
            break

        except Exception:
            print("Try again")

    while True:
        query = "Please enter an END DATE in the following format [ MM/DD/YY HH:MM ] e.g. 01/01/19 23:00 \n" \
                "All requests should be entered in UTC & 24hr notation\n" \
                "Or leave blank for current time.\n" \
                "End:"
        try:
            end_time = input(query)
            if end_time == "":
                end_time = datetime.datetime.now(tz=pytz.utc)
                print(end_time)
                break
            else:
                end_time = datetime.datetime.strptime(end_time, "%m/%d/%y %H:%M")
                end_time = end_time.replace(tzinfo=pytz.timezone('UTC'))
                print(end_time)
                break
        except Exception:
            print("Try again")
            continue

    start_time = int((start_time - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds())
    end_time = int(candle_size * ((end_time - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
                                  // candle_size) + candle_size)

    interpolate = menu_structure("Would you like to use interpolation to fill in missing data? \n"
                                 "*Note* Interpolated data will be designated as such in the csv file.",
                                 "Yes", "No")

    if interpolate is 'Yes':
        interpolate_method = menu_structure("Which interpolation method would you like to use? \n",
                                            "linear", "spline", "polynomial")
        if interpolate_method is "spline" or interpolate_method is "polynomial":
            order = get_non_negative_int("Please specify an order. Enter a number: ")

    # Start Import #

    request_size = 200

    print("There are {:,} candles to import. This will require {:,} requests to Coinbase."
          .format(int((end_time - start_time) / candle_size),
                  int(math.ceil(((end_time - start_time) / candle_size) / request_size))))

    pend_data = []

    for epoch in tqdm.tqdm(range(start_time, end_time, candle_size * request_size)):

        request_start = epoch
        request_end = epoch + (candle_size * request_size)
        retry = 0

        while retry < 3:
            if request_end > end_time:
                request_end = end_time
            try:
                time.sleep(1)
                data = public_client.get_product_historic_rates(product_id=product,
                                                                start=time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                                                                    time.gmtime(request_start)),
                                                                end=time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                                                                  time.gmtime(request_end)),
                                                                granularity=candle_size)
                if isinstance(data, list) == False:
                    raise Exception(data)

                else:
                    pend_data.extend(data)
                    break

            except Exception as e:
                print("Import Error", e)
                retry += 1
                time.sleep(1.3)

    # Create list of epochs to compare returned candles to
    master_epoch_list = [epoch for epoch in range(start_time, end_time, candle_size)]
    imported_epoch_list = [epoch[0] for epoch in pend_data]
    missing_epochs = [epoch for epoch in master_epoch_list if epoch not in imported_epoch_list]

    pend_data.sort()

    # Deduplicate pend_data
    b = []
    [b.append(candle) for candle in pend_data if candle not in b]
    pend_data = b

    # Delete candles that fall outside the range we need. ie extra candles returned from GDAX.
    pend_data = [candle for candle in pend_data if candle[0] in master_epoch_list]

    print("Successfully fetched {} records ranging {} UTC to {} UTC".format(len(pend_data),
                                                                            datetime.datetime.utcfromtimestamp(
                                                                                pend_data[0][0]),
                                                                            datetime.datetime.utcfromtimestamp(
                                                                                pend_data[-1][0])))

    print("{} Records were not returned from Coinbase for the requested period.".format(len(missing_epochs)))

    # Data Cleanup
    df = pd.DataFrame(pend_data)
    df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    df.set_index('time', inplace=True)
    df.index = pd.to_datetime(df.index, unit='s')

    if interpolate == 'Yes' and len(missing_epochs) != 0:
        mf = pd.DataFrame(missing_epochs)
        mf.columns = ['time']
        mf.set_index("time", inplace=True)
        mf.index = pd.to_datetime(mf.index, unit='s')

        mf['interpolated'] = 'x'
        df['interpolated'] = " "
        df = pd.concat([df, mf], sort=True)
        df.sort_index(inplace=True)
        if interpolate_method is "spline" or interpolate_method is "polynomial":
            df.interpolate(inplace=True, method=interpolate_method, order=order)
        else:
            df.interpolate(inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume', 'interpolated']]

    # Abandoned (for now) code to drop extra decimals past Coinbase Pro's min_size for the quote currency.
    # Mostly an issue with interpolation and the long decimal numbers that result from calcs.

    #    currencies = public_client.get_currencies()
    #    currency_dict = {}
    #    for pair in currencies:
    #        currency_dict[pair["id"]] = pair
    #    min_quote_size = currency_dict[product[4:]]["min_size"]
    #    precision = len(min_quote_size.split('.'))
    #    df.style.set_precision(precision)

    else:
        df.sort_index(inplace=True)

    filename = "{} {} UTC to {} UTC - {} {}.csv".format(product, df.index[0].strftime("%Y-%m-%d %H%M"),
                                                df.index[-1].strftime("%Y-%m-%d %H%M"), candle_sizer, interpolate_method)
    df.to_csv(filename, mode='w+')
    print(filename, "created successfully.")


if __name__ == "__main__":
    print("Hello, and welcome to the Coinbase Pro Historical Candle Importer.")
    import_new()
