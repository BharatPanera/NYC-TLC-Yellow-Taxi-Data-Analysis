import happybase

connection = happybase.Connection('localhost', port=9090, autoconnect=False)

def open_connection():
    connection.open()

def close_connection():
    connection.close()

def get_table():
    open_connection()
    table = connection.table('taxi_tripdata_hbase')
    close_connection()
    return table

def batch_insert_data(filename, start_count):
    print("Start batch insert of Trip Data for file: ", filename)
    file = open(filename, "r")
    table = get_table()
    open_connection()


    row_key = start_count
    i=0
    with table.batch(batch_size = 1000) as b:
        for line in file:
            if i != 0: 
                cols = line.strip().split(",")
                b.put(row_key,{
                    "trip_info:VendorID": cols[0],
                    "trip_info:tpep_pickup_datetime": cols[1],
                    "trip_info:tpep_dropoff_datetime": cols[2],
                    "trip_info:Passenger_count": cols[3],
                    "trip_info:Trip_distance": cols[4],
                    "trip_info:RateCodeID": cols[5],
                    "trip_info:Store_and_fwd_flag": cols[6],
                    "trip_info:PULocationID": cols[7],
                    "trip_info:DOLocationID": cols[8],
                    "trip_info:Payment_type": cols[9],
                    "trip_info:Fare_amount": cols[10],
                    "trip_info:Extra": cols[11],
                    "trip_info:MTA_tax": cols[12],
                    "trip_info:Tip_amount": cols[13],
                    "trip_info:Tolls_amount": cols[14],
                    "trip_info:Improvement_surcharge": cols[15],
                    "trip_info:Total_amount": cols[16],
                    "trip_info:Congestion_Surcharge": cols[17],
                    "trip_info:Airport_fee": cols[18]
                })

                row_key = str(int(row_key) + 1)

            i += 1
    file.close()
    print("Batch insert completed for the file: ", filename)
    close_connection()
    return row_key

row1 = batch_insert_data('yellow_tripdata_2017-03.csv', '18880596')
row2 = batch_insert_data('yellow_tripdata_2017-04.csv', row1)
print("Total row in taxi_tripdata_hbase table: ", (int(row2) - 1))