from fastapi import FastAPI
import requests
import json
import pandas as pd
import numpy as np
import os
import datetime

app = FastAPI()
def read_mutual_fund_list():
    df_mf_list = pd.read_csv('https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0', header = 'infer')
    df_mf_list.columns = df_mf_list.columns.str.strip()
    df_mf_list.replace(np.nan,'', inplace = True)
    df_mf_list_1 = df_mf_list[(df_mf_list['Scheme NAV Name'].str.contains("direct", case=False)) & (df_mf_list['Scheme NAV Name'].str.contains("growth", case=False))].reset_index(drop=True)
    df_mf_list_1.to_csv('mf_data_list.csv', index=False)
    return df_mf_list_1

def get_mf_price_data_mfapi(code, ):
    import time
    max_retries = 5
    backoff = 2
    for attempt in range(max_retries):
        try:
            mf_data_response = requests.get(f"https://api.mfapi.in/mf/{code}", allow_redirects=True, timeout=10)
            if mf_data_response.status_code == 200:
                break
            else:
                print(f"Non-200 response for code {code}: {mf_data_response.status_code}")
        except requests.RequestException as e:
            print(f"Request failed for code {code} (attempt {attempt+1}): {e}")
        if attempt < max_retries - 1:
            sleep_time = backoff ** attempt
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    else:
        print(f"Failed to fetch data for code {code} after {max_retries} attempts.")
        return pd.DataFrame()

    mf_data_df = pd.DataFrame()
    try:
        mf_data_1 = json.loads(mf_data_response.content.decode('utf-8'))
        # mf_data = pd.json_normalize(mf_data_1['data'])
        mf_data = pd.json_normalize(mf_data_1)
        mf_data_df = pd.DataFrame.from_dict(mf_data['data'][0])
        print("inside get_mf_price_data_mfapi")
        if mf_data_df.empty:
            print(f"No data found for code: {code}")
        else:
            print(mf_data_df.columns)
            mf_data_df['date'] = pd.to_datetime(mf_data_df['date'], format = '%d-%m-%Y')
            mf_data_df['nav'] = mf_data_df['nav'].astype(float)
            mf_data_df = mf_data_df.sort_values('date')
            mf_data_df['daily_returns'] = mf_data_df['nav'].pct_change()
            mf_data_df['cumulative_returns'] = (mf_data_df['daily_returns']+1).cumprod()
    except Exception as e:
        print(f"Error processing data for code {code}: {e}")
        return pd.DataFrame()
    return mf_data_df

def calculate_return_by_timeframe(returns_df, df, no_of_years):
    cutoff = datetime.datetime.now() - datetime.timedelta(days=no_of_years*365)
    # filtered_schemes = df.groupby('Scheme Code')['date'].min()
    # filtered_schemes = filtered_schemes[filtered_schemes <= cutoff].index
    # filtered_df = df[df['Scheme Code'].isin(filtered_schemes)]

    col_name_total = f'{no_of_years}Y Return'
    col_name_cagr = f'{no_of_years}Y CAGR %'
    
    data = {
        'scheme_code': [],
        'scheme_name': [],
        # col_name_total: [],
        col_name_cagr: []
    }
    
    returns = []

    now = pd.Timestamp.now()
    for scheme, group in df.groupby('Scheme Code'):
        print(f"Processing scheme: {scheme}")
        group = group.copy()
        # Ensure 'date' column is datetime
        if not pd.api.types.is_datetime64_any_dtype(group['date']):
            group['date'] = pd.to_datetime(group['date'], errors='coerce')
        # Ignore funds whose last NAV date is not within the last 3 days
        last_nav_date = group['date'].max()
        if (now - last_nav_date).days > 3:
            print(f"Skipping scheme {scheme} due to stale NAV date: {last_nav_date}")
            continue
        if group['date'].min() >= cutoff:
            data['scheme_code'].append(scheme)
            data['scheme_name'].append(group.iloc[0]['Scheme Name'])
            data[col_name_cagr].append(0)
        else:
            group_sorted = group.sort_values('date')
            filtered = group_sorted.loc[group_sorted['date'] >= cutoff]
            if filtered.empty:
                data['scheme_code'].append(scheme)
                data['scheme_name'].append(group.iloc[0]['Scheme Name'])
                data[col_name_cagr].append(0)
                continue
            start_val = filtered.iloc[0]['nav']
            end_val = group_sorted.iloc[-1]['nav']
            total_return = (end_val / start_val - 1) * 100
            cagr = ((1 + total_return / 100) ** (1 / no_of_years) - 1) * 100
            data['scheme_code'].append(scheme)
            data['scheme_name'].append(group.iloc[0]['Scheme Name'])
            data[col_name_cagr].append(cagr)

    # Now add all at once
    for col, values in data.items():
        returns_df[col] = values
    return returns_df

@app.get("/get_all_mutual_funds")
def get_all_mutual_funds():
    df_mf_list = read_mutual_fund_list()
    return df_mf_list.to_dict(orient='records')

@app.get("/get_all_mutual_funds_categories")
def get_all_mutual_funds_categories():
    df_mf_list = read_mutual_fund_list()
    scheme_categories = df_mf_list['Scheme Category'].unique()
    return sorted(scheme_categories.tolist())

@app.get("/get_all_AMC")
def get_all_AMC():
    df_mf_list = read_mutual_fund_list()
    amc_list = df_mf_list['AMC'].unique()
    return sorted(amc_list.tolist())

@app.get("/get_all_mutual_funds_by_category/{category}")
def get_all_mutual_funds_by_category(category: str):
    df_mf_list = read_mutual_fund_list()
    df_filtered = df_mf_list[df_mf_list['Scheme Category'] == category]
    return df_filtered.to_dict(orient='records')

@app.get("/get_top20_mutual_funds_by_return_1year")
def get_top20_mutual_funds_by_return_1year():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    all_funds = []
    i = 0
    df_mf_list = read_mutual_fund_list()
    if os.path.exists('mf_data.csv'):
        print('file already exist')
        final_df = pd.read_csv("mf_data.csv")
    else:
        print(len(df_mf_list))
        def fetch_fund(row):
            code = row['Code']
            print(f"{code} - {row['Scheme Name']}")
            mf_data_df = get_mf_price_data_mfapi(code)
            print(mf_data_df.columns)
            if mf_data_df.empty:
                return None
            mf_data_df['Scheme Code'] = row['Code']
            mf_data_df['Scheme Name'] = row['Scheme Name']
            mf_data_df['Scheme Category'] = row['Scheme Category']
            print(f"{code} - {row['Scheme Name']} step 1")
            print(f"{code} - {row['Scheme Name']} step 2")
            print(mf_data_df.columns)
            return mf_data_df

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(fetch_fund, row) for _, row in df_mf_list.iterrows()]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_funds.append(result)
                    i += 1
                    print(i)
        print("this is the place")
        if all_funds:
            final_df = pd.concat(all_funds, ignore_index=True)
            final_df = final_df[['Scheme Code', 'Scheme Name', 'Scheme Category', 'date', 'nav' , 'daily_returns', 'cumulative_returns']]
            final_df['date'] = pd.to_datetime(final_df['date'])
            final_df.to_csv("mf_data.csv")
        else:
            print("No fund data fetched.")
            return []
    returns_df = pd.DataFrame()
    returns_df = calculate_return_by_timeframe(returns_df, final_df, 1)
    returns_df.sort_values('1Y CAGR %',ascending=False, inplace=True, ignore_index=True)

    return returns_df.head(20).to_dict(orient='records')

@app.get("/get_top20_mutual_funds_by_return_3year")
def get_top20_mutual_funds_by_return_3year():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    all_funds = []
    i = 0
    df_mf_list = read_mutual_fund_list()
    if os.path.exists('mf_data.csv'):
        print('file already exist')
        final_df = pd.read_csv("mf_data.csv")
    else:
        print(len(df_mf_list))
        def fetch_fund(row):
            code = row['Code']
            print(f"{code} - {row['Scheme Name']}")
            mf_data_df = get_mf_price_data_mfapi(code)
            print(mf_data_df.columns)
            if mf_data_df.empty:
                return None
            mf_data_df['Scheme Code'] = row['Code']
            mf_data_df['Scheme Name'] = row['Scheme Name']
            mf_data_df['Scheme Category'] = row['Scheme Category']
            print(f"{code} - {row['Scheme Name']} step 1")
            print(f"{code} - {row['Scheme Name']} step 2")
            print(mf_data_df.columns)
            return mf_data_df

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(fetch_fund, row) for _, row in df_mf_list.iterrows()]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_funds.append(result)
                    i += 1
                    print(i)
        print("this is the place")
        if all_funds:
            final_df = pd.concat(all_funds, ignore_index=True)
            final_df = final_df[['Scheme Code', 'Scheme Name', 'Scheme Category', 'date', 'nav' , 'daily_returns', 'cumulative_returns']]
            final_df['date'] = pd.to_datetime(final_df['date'])
            final_df.to_csv("mf_data.csv")
        else:
            print("No fund data fetched.")
            return []
    returns_df = pd.DataFrame()
    returns_df = calculate_return_by_timeframe(returns_df, final_df, 10)
    returns_df.sort_values('10Y CAGR %',ascending=False, inplace=True, ignore_index=True)

    return returns_df.head(20).to_dict(orient='records')

if __name__ == "__main__":
    print("I am here")