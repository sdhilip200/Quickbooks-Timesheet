import requests
import pandas as pd
from datetime import datetime, timedelta
import os

def get_timesheet_data(url, headers):
    """Fetch data from API"""
    try:
        response = requests.get(url, headers=headers)
        return response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def parse_timesheet(api_data):
    """Convert API data to DataFrame"""
    data_list = []
    
    if not api_data or 'results' not in api_data or 'timesheets' not in api_data['results']:
        return pd.DataFrame()
    
    for key, data in api_data['results']['timesheets'].items():
        record = {
            'timesheetid': data.get('id', ''),
            'user_id': data.get('user_id', 0),
            'jobcode_id': data.get('jobcode_id', 0),
            'start_date': data.get('start'),
            'end_date': data.get('end'),
            'duration': data.get('duration', 0),
            'date': data.get('date'),
            'tz': data.get('tz'),
            'tz_str': data.get('tz_str'),
            'type': data.get('type', ''),
            'location': data.get('location', ''),
            'on_the_clock': data.get('on_the_clock', ''),
            'notes': data.get('notes', ''),
            'last_modified': data.get('last_modified'),
            'created_by_user_id': data.get('created_by_user_id', 0),
            'load_timestamp': datetime.now()
        }
        data_list.append(record)
    
    return pd.DataFrame(data_list)

def extract_timesheet_data(full_load='N'):
    """Main function to extract timesheet data"""
    # API Configuration
    token = 'xxxxxxx'  # Replace with your API token
    headers = {'Authorization': f'Bearer {token}'}
    
    # Initialize empty DataFrame for all data
    all_data = pd.DataFrame()
    page = 1
    
    while True:
        try:
            # Set URL based on load type
            if full_load == 'Y':
                # Full load - last 2 years of data
                start_date = "2024-01-01"
                end_date = "2024-01-31"
                url = f"https://rest.tsheets.com/api/v1/timesheets?start_date={start_date}&end_date={end_date}&on_the_clock=both&per_page=50&page={page}"
            else:
                # Incremental load - last 24 hours
                modified_since = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%S-06:00')
                url = f"https://rest.tsheets.com/api/v1/timesheets?modified_since={modified_since}&on_the_clock=both&per_page=50&page={page}"

            # Get data from API
            print(f"Fetching page {page}")
            timesheet_data = get_timesheet_data(url, headers)
            
            if not timesheet_data:
                break
            
            # Parse data and append to main DataFrame
            current_df = parse_timesheet(timesheet_data)
            all_data = pd.concat([all_data, current_df], ignore_index=True)
            
            # Check if there are more pages
            if not timesheet_data.get('more', False):
                break
                
            page += 1
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            break
    
    return all_data

def save_to_lakehouse(df, full_load='N'):
    """Save DataFrame to Lakehouse"""
    try:
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Set filename based on load type
        if full_load == 'Y':
            filename = f'timesheet_full_load_{timestamp}.csv'
        else:
            # Incremental load - Reflecting the start of the last 24-hour period
            incremental_date = (datetime.now() - timedelta(hours=24)).strftime('%Y%m%d')
            filename = f'{incremental_date}.csv'
        
        # Save to lakehouse
        lakehouse_path = 'abfss://xxxxxxxx@onelake.dfs.fabric.microsoft.com/xxxxxx/Files/'  # Adjust path as needed
                    
        # Save file
        full_path = os.path.join(lakehouse_path, filename)
        df.to_csv(full_path, index=False)
        print(f"Data saved to {full_path}")
        print(f"Total records saved: {len(df)}")
        
    except Exception as e:
        print(f"Error saving to lakehouse: {e}")

def main(full_load='N'):
    """Main execution function"""
    print(f"Starting {'full' if full_load == 'Y' else 'incremental'} load at {datetime.now()}")
    
    # Extract data
    df = extract_timesheet_data(full_load)
    
    if not df.empty:
        # Save to lakehouse
        save_to_lakehouse(df, full_load)
        print(f"Process completed successfully at {datetime.now()}")
    else:
        print("No data extracted")

# Example usage
if __name__ == "__main__":
    # Set full_load to 'Y' for full load, 'N' for incremental
    load_type = 'Y'  
    main(load_type)