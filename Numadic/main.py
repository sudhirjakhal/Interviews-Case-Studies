from flask import Flask, request, jsonify, send_file
from math import radians, cos, sin, asin, sqrt
import pandas as pd
import zipfile
import os

# profile the code
import cProfile
import pstats

app = Flask(__name__)

@app.route('/')
def homepage():
    return jsonify({'API call syntax':'http://<your_api_url>/your_endpoint?start_time=<your_start_time>&end_time=<your_end_time>'})

@app.route("/asset_report")
def generate_asset_report():
    # Fetching start_time and end_time from the URL using GET method
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    # Check if start_time and end_time are not None
    if start_time is None or end_time is None:
       return jsonify({'error': 'provide the required paramenters start_time and end_time'}), 400

    # Check if start_time and end_time are in valid epoch format
    try:
        start_time = int(start_time)
        end_time = int(end_time)
    except ValueError:
        return jsonify({'error': 'invalid epoch format for start_time and end_time'}), 400

    try:
        vehicle_trips = read_csv_trip_info(start_time, end_time)
        vehicle_trails = read_location_zip(start_time, end_time)
    except Exception as e:
        return jsonify({'Error': f"{e}"})
    
     # Compute the distance travelled by each vehicle
    vehicle_distances = compute_distance(vehicle_trails)
    
    # compute number of trips completed
    trips_completed = vehicle_trips.groupby('vehicle_number').size().reset_index(name='trips_completed')
    
    # compute average speed and number of speed violations
    speed_data = vehicle_trails.groupby(['fk_asset_id', 'lic_plate_no']).agg({'spd': 'mean', 'osf': 'sum'}).rename(columns={'spd': 'average_speed', 'osf': 'speed_violations'}).reset_index()

    transporter_names = vehicle_trips.groupby('vehicle_number')['transporter_name'].first().to_frame().reset_index()

    # Join all the computed dataframes to create the final report
    vehicle_report = pd.merge(vehicle_distances, speed_data, on='fk_asset_id', how='left')
    vehicle_report = pd.merge(vehicle_report, trips_completed.rename(columns={'vehicle_number': 'lic_plate_no'}), on='lic_plate_no', how='left')
    vehicle_report = pd.merge(vehicle_report, transporter_names.rename(columns={'vehicle_number': 'lic_plate_no'}), on='lic_plate_no', how='left')

    if vehicle_report.empty:
        return jsonify({f'Error':f'No data found for start time : {start_time} and end time : {end_time}'})
    else:
        # Renaming  and reorder columns
        vehicle_report = vehicle_report.rename(columns = {'lic_plate_no':'License plate number', 'distance':'Distance', 'trips_completed':'Number of Trips Completed', 'spd':'Average Speed', 'transporter_name':'Transporter Name', 'osf':'Number of Speed Violations'}, inplace = True)

        # Saving the dataframe into Excel file
        vehicle_report.to_excel('asset_report.xlsx', index=False)

        return send_file('asset_report.xlsx', as_attachment=True)


def read_csv_trip_info(start_time, end_time):
    """
    Read the 'Trip-Info.csv' file and filter its contents based on the given time range.

    Args:
        start_time (int): The start time of the time range in epoch format.
        end_time (int): The end time of the time range in epoch format.

    Returns:
        A pandas DataFrame containing the filtered contents of the 'Trip-Info.csv' file.

    Raises:
        FileNotFoundError: If the 'Trip-Info.csv' file does not exist at the specified path.
    """
    # joining working directory path with filename
    trip_info_csv = os.path.join(os.path.dirname(__file__), 'files', 'Trip-Info.csv')
    # Check if file exists or not
    if not os.path.exists(trip_info_csv):
        raise FileNotFoundError(f"{trip_info_csv}: File not found.")
    else:
        trip_info_df = pd.read_csv(trip_info_csv)
        # Filter the dataframe based on start time and end time
        trip_info_filtered = trip_info_df.loc[start_time:end_time]
        return trip_info_filtered


def read_location_zip(start_time, end_time):
    """
    Read the 'NU-raw-location-dump.zip' file, extract its contents, and filter them based on the given time range.

    Args:
        start_time (int): The start time of the time range in epoch format.
        end_time (int): The end time of the time range in epoch format.

    Returns:
        A pandas DataFrame containing the filtered contents of all CSV files in the 'NU-raw-location-dump.zip' archive.

    Raises:
        FileNotFoundError: If the 'NU-raw-location-dump.zip' file does not exist at the specified path.
    """
    columns = ['harsh_acceleration','hbk','lat','lname','lon','osf','spd','tis','fk_asset_id','lic_plate_no']
    file_list = []
    with zipfile.ZipFile(os.path.join(os.path.dirname(__file__), 'files', 'NU-raw-location-dump.zip')) as zip_file:
        for file_name in zip_file.namelist():
            if file_name != 'EOL-dump/':
                with zip_file.open(file_name) as csv_file:
                    df = pd.read_csv(csv_file, low_memory=False,usecols = columns)
                    #Remove Null values
                    df = df.loc[df['lat'].notna() & df['lon'].notna() & df['spd'].notna() & df['lname'].notna()]
                    filtered = df.loc[start_time:end_time]
                    file_list.append(filtered)
    location_df = pd.concat(file_list)
    return location_df

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def compute_distance(vehicle_trails):
    # create an empty dataframe to store the distances
    distance_df = pd.DataFrame(columns=['fk_asset_id', 'distance'])
    distance_list = []
    # iterate over unique fk_asset_ids in vehicle_trails
    for asset_id in vehicle_trails['fk_asset_id'].unique()[:1]:
    
        # select the rows in vehicle_trails for the current fk_asset_id
        asset_rows = vehicle_trails[vehicle_trails['fk_asset_id'] == asset_id]
    
        # initialize distance to zero
        distance = 0
    
        # iterate over the rows for the current fk_asset_id, skipping the first row
        for i in range(1, len(asset_rows)):
        
            # compute the distance between the current row and the previous row
            lat1, lon1 = asset_rows.iloc[i-1]['lat'], asset_rows.iloc[i-1]['lon']
            lat2, lon2 = asset_rows.iloc[i]['lat'], asset_rows.iloc[i]['lon']
            distance += haversine(lat1, lon1, lat2, lon2)
        
        # add the current fk_asset_id and distance to the distance_df dataframe
        distance_list.append(pd.DataFrame([{'fk_asset_id': asset_id, 'distance': distance}]))

    if len(distance_list) == 0:
        distance_df = pd.DataFrame(columns=['fk_asset_id','distance'])
    else:
        distance_df = pd.concat(distance_list, ignore_index=True)
    
    return distance_df


# main driver function
if __name__ == '__main__':
    app.run(debug=True)
