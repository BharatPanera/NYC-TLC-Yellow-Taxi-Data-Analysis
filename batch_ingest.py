# Importing the HappyBase library for interaction with HBase
import happybase

# Establishing connection to HBase
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# Function to open the connection to HBase
def open_connection():
    connection.open()

# Function to close the connection to HBase
def close_connection():
    connection.close()

# Function to get the table from HBase
def get_table():
    open_connection()
    table = connection.table('taxi_tripdata_hbase')
    close_connection()
    return table

# Function to batch insert data into HBase table from a file
def batch_insert_data(filename, start_count):
    print("Starting batch insert of Trip Data for file:", filename)
    file = open(filename, "r")
    table = get_table()
    open_connection()
    # Next row key for the HBase table after the last data ingest
    row_key = start_count
    i = 0
    with table.batch(batch_size=1000) as b:
        for line in file:
            if i != 0:  # To omit header line with column names
                cols = line.strip().split(",")
                b.put(row_key, {
                    "trip_details:vendorid": cols[0],
                    "trip_details:tpep_pickup_datetime": cols[1],
                    "trip_details:tpep_dropoff_datetime": cols[2],
                    "trip_details:passenger_count": cols[3],
                    "trip_details:trip_distance": cols[4],
                    "trip_details:ratecodeid": cols[5],
                    "trip_details:store_and_fwd_flag": cols[6],
                    "trip_details:pulocationid": cols[7],
                    "trip_details:dolocationid": cols[8],
                    "invoice_details:payment_type": cols[9],
                    "invoice_details:fare_amount": cols[10],
                    "invoice_details:extra": cols[11],
                    "invoice_details:mta_tax": cols[12],
                    "invoice_details:tip_amount": cols[13],
                    "invoice_details:tolls_amount": cols[14],
                    "invoice_details:improvement_surcharge": cols[15],
                    "invoice_details:total_amount": cols[16],
                    "invoice_details:congestion_surcharge": cols[17],
                    "invoice_details:airport_fee": cols[18]
                })

                row_key = str(int(row_key) + 1)

            i += 1
    file.close()
    print("Batch insert completed for file:", filename)
    close_connection()
    return row_key

# Inserting data from file to taxi_tripdata_hbase table
# Start counter is 501 as the current number of rows in HBase table after SQL import is 18880595

rows1 = batch_insert_data('yellow_tripdata_2017-03.csv', '18880596')
rows2 = batch_insert_data('yellow_tripdata_2017-04.csv', rows1)
print('Total rows in taxi_tripdata_hbase table:', (int(rows2) - 1))