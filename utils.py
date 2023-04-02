import mysql.connector
import isodate
from datetime import datetime, timezone

def get_connection(host, database, port, user, password):
    """ Create connection to MySQL """

    try:
        conn = mysql.connector.connect(host=host, 
                                       database=database, 
                                       port=port, 
                                       user=user, 
                                       password=password
                                       )
        # Check if the connection was successful
        if conn.is_connected():
            print('Connected to MySQL database')
            return conn

    except mysql.connector.Error as e:
        print('Error connecting to MySQL database: {}'.format(e))
        raise


def insert_data(connection, data, insert_query):
    """ Insert dataframe values to database """

    try:
        # Create a cursor object
        cursor = connection.cursor()

        # Get the values from the DataFrame as a list of tuples
        values = data.values.tolist()

        # Execute the insert query for all rows at once
        cursor.executemany(insert_query, values)

        # Commit the changes
        connection.commit()
        cursor.close()
        print('Finished insert_data.')

    except Exception as e:
        print(f'Found error in inserting_data: {e}')
        raise


def duration_to_second(duration: str) -> int:
    """ Convert ISO 8601 duration to number of seconds """

    duration_sec = isodate.parse_duration(duration).total_seconds()

    return int(duration_sec)

def utc_to_local(utc_datetime: str) -> str:
    """ Convert ISO 8601 datetime to local datetime """

    utc_time = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
    local_time = utc_time.replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%Y-%m-%d %H:%M:%S')

    return local_time

def get_current_time() -> str:
    """ Return string of current time in the format of %Y-%m-%d %H:%M:%S """

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return now

def covert_to_millions(count: str) -> float:
    """ Convert a string containing numbers to a number in millions """

    try:
        # extract digits from input string
        count_digits = ''.join(c for c in count if c.isdigit())
        # convert to float and divide by 1 million
        count_m = round(float(count_digits) / 1E6, 2)
    except:
        count_m = ""
    
    return count_m
