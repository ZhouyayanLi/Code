import json
import requests
import pandas as pd
from datetime import date, datetime
import country_converter as coco

# Q1 
years = [2023, 2024]
country = 'US'
API_head = 'https://date.nager.at/api/v3/publicholidays/'


def getData(year_range, country):
    # if given a single year
    if isinstance(year_range, int):
        response = requests.get(API_head+str(year_range)+'/'+country)
        public_holidays = json.loads(response.content)

        df = pd.DataFrame(public_holidays)
        df.types = df.types.str.join('')
        df.counties = df.counties.str.join('')
        return df
    else:
        # if given a list of years
        frames = []
        for year in year_range:
            response = requests.get(API_head+str(year)+'/'+country)
            public_holidays = json.loads(response.content)

            df = pd.DataFrame(public_holidays)
            df.types = df.types.str.join('')
            df.counties = df.counties.str.join('')

            frames.append(df)
            newFrame = pd.concat(frames)
            newFrame = newFrame.reset_index(drop=True)
        return newFrame

    
result = getData(years, country)
#print(result)

filePath = 'hoilday.csv'
result.to_csv(filePath, index=False)


# Q2
date_format = "%Y-%m-%d"
newDate_format = '%A, %B %d, %Y'
today = date.today()

filePath_input = 'hoilday.csv'
readin = pd.read_csv(filePath_input)

def convert_date(date_string):
    date = datetime.strptime(date_string, date_format)
    return date.strftime(newDate_format)


def holidays():
    for index, row in readin.iterrows():
        if datetime.strptime(row.date, date_format).date() > today:
            end = index + 10 - 1
            # the dataframe contains more than 10 upcoming holidays
            if end < readin.shape[0]: 
                upcoming = readin.iloc[index:end+1, [2, 0, 8, 5]]
            # if less than 10 upcoming holidays remain, return all
            else:
                upcoming = readin.iloc[index:, [2, 0, 8, 5]]

            # convert the format of date to the assigned one
            upcoming.date = upcoming.date.apply(convert_date)
            
            # change the format of the return values
            return upcoming.to_dict(orient='records')

print(holidays())


# Q3
# This question is a bit vague for me. One understanding is that we are to build a filter 
# to screen the holidays return in Q2 with the global tag equals to the given value.   
# In the above case, we may get less than 10 holidays.

# Another understanding is that we keep looking for and add into our current list records that 
# match the global tag until we get 10 hoildays.
# In the latter case, unless we run out of records, we will always be able to get 10 upcoming holidays.

# The implementation of the first version, just filtering the values we got in Q2
# def holidays(global_flag=None):
#     rows = readin
#     for index, row in rows.iterrows():
#         if datetime.strptime(row.date, date_format).date() > today:
#             end = index + 10 - 1
#             # the dataframe contains more than 10 upcoming holidays
#             if end < rows.shape[0]: 
#                 upcoming = rows.iloc[index:end+1, [2, 0, 8, 5]]
#             # if less than 10 upcoming holidays remain, return all
#             else:
#                 upcoming = rows.iloc[index:, [2, 0, 8, 5]]

#             # convert the format of date to the assigned one
#             upcoming.date = upcoming.date.apply(convert_date)
            
#             if global_flag != None:
#                 upcoming = upcoming[upcoming["global"] == global_flag]  
            
#             # change the format of the return values
#             return upcoming.to_dict(orient='records')

# The implementation of the second version, get 10 upcoming holidays with the assigned global tag
def holidays(global_flag=None):
    rows = readin
    
    if global_flag != None:
        
        # filter the data with the global tag
        rows = rows[rows["global"] == global_flag]
        # reset index for the new table
        rows = rows.reset_index(drop=True)
        
    
    for index, row in rows.iterrows():
        if datetime.strptime(row.date, date_format).date() > today:
            end = index + 10 - 1

            # the dataframe contains more than 10 upcoming holidays
            if end < rows.shape[0]: 
                upcoming = rows.iloc[index:end+1, [2, 0, 8, 5]]
            # if less than 10 upcoming holidays remain, return all
            else:
                upcoming = rows.iloc[index:, [2, 0, 8, 5]]


            # convert the format of date to the assigned one
            upcoming.date = upcoming.date.apply(convert_date)
            
            # change the format of the return values
            return upcoming.to_dict(orient='records')
        
print(holidays(True))



# Q4.

# The workflow goes like this:
# If the comparison country is not provided, just return the situation in US.
# Otherwise, get the first 10 upcoming holidays from both countries (apply global flag filter if required)
# and then inner join the two table.
# Finally, apply the global flag on the joined table

today = date.today()

def holidays_country2(comparison_country):
    if not comparison_country:
        return None
    standard_names = coco.convert(names=comparison_country, to='ISO2')
    return getData(years, standard_names)


def holidays(global_flag=None, comparison_country=None):
 
    def get_global(data_in):
        if isinstance(data_in, pd.DataFrame):
            # filter the data with the global tag
            data_out = data_in[data_in["global"] == global_flag]
            # reset index for the new table
            data_out = data_out.reset_index(drop=True)
            return data_out
        else:
            return data_in
    
    # get data for the US
    US_data = holidays_country2('US')
    comparison_data = holidays_country2(comparison_country)
    
    
    if global_flag != None:
        US_data = get_global(US_data)
        comparison_data = get_global(comparison_data)
    
    # if no data was fetched for the comparison country (comparison country is not provided)
    if not isinstance(comparison_data, pd.DataFrame):
        rows = US_data
    else:
        rows = pd.merge(US_data[["name", "date", "types", "global"]], comparison_data[["localName", "date", "types", "global"]], on="date")
        rows = rows[["name", "localName","date", "types_x", "types_y", "global_x", "global_y"]]
        rows = rows.rename(columns={ "localName": "comparison_name","types_x": "type", "types_y": "comparison_type", "global_x": "global", "global_y": "comparison_global"})
    
    for index, row in rows.iterrows():
        #print(row.date)
        if datetime.strptime(row.date, date_format).date() > today:
            end = index + 10 - 1

            # the dataframe contains more than 10 upcoming holidays
            if end < rows.shape[0]: 
                upcoming = rows.iloc[index:end+1]
            # if less than 10 upcoming holidays remain, return all
            else:
                upcoming = rows.iloc[index:]


            # convert the format of date to the assigned one
            #print(upcoming.shape[0])
            upcoming.date = upcoming.date.apply(convert_date)
            
            # change the format of the return values
            return upcoming.to_dict(orient='records')
print(holidays(global_flag=True, comparison_country="Mexico"))   


# Q5
countries = requests.get('https://date.nager.at/api/v3/AvailableCountries')
country_list = json.loads(countries.content)
country_names = [item['name'] for item in country_list]

country_holidays = {}

# iterate over the countries and save in the country_holidays dictionary
# the country code and the houliday name list
def get_next_holidays(country_name):
    country_holidays[country_name] = []
    country_code = coco.convert(names=country_name, to='ISO2')
    url = 'https://date.nager.at/api/v3/NextPublicHolidays/' + country_code
    response = requests.get(url)
    holidays = response.json()
    
    for holiday in holidays:
        # adding the two part together to avoid when date and the neme 
        # are not the same at the same time with the comparison target's items 
        country_holidays[country_name].append(holiday['name']+holiday['date'])

for country_name in country_names:
    get_next_holidays(country_name)
#print(country_holidays)

def most_common_countries(comparison_country="United States"):
    # Create a list of (key, common_items_count) pairs
    common_items_counts = [(country_name, len(set(value) & set(country_holidays[comparison_country]))) for country_name, value in country_holidays.items() if country_name != comparison_country]
    
    # Sort the list in descending order of common_items_count
    common_items_counts.sort(key=lambda x: x[1], reverse=True)
    
    # Get the first 10 keys from the sorted list
    top_10_countries = [{"counry": item[0], "common_dates": item[1], "rank": idx+1} for idx, item in enumerate(common_items_counts[:10])]
    
    return top_10_countries
print(most_common_countries())