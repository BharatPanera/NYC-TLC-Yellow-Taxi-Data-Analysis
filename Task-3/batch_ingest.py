import happybase
import logging
import time

# Setting up logging
logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO)
# logging.disable()

class HBaseClient:
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self.connection = happybase.Connection(host=self.host, port=self.port, autoconnect=False)
        self.open_connection()  # Open connection upon initialization

    def open_connection(self):
        self.connection.open()
        logging.info("Connection to HBase opened.")

    def close_connection(self):
        self.connection.close()
        logging.info("Connection to HBase closed.")

    def get_table(self, name):
        table = self.connection.table(name)
        return table

    def batch_insert_data(self, filename, tablename, batch_size=50000):
        logging.info("Starting batch insert of %s", filename)
        start_time = time.time()
        try:
            with open(filename, 'r') as file:
                table = self.get_table(tablename)
                with table.batch(batch_size=batch_size) as b:
                    for i, line in enumerate(file):
                        if i != 0:
                            temp = line.strip().split(",")
                            row_key = temp[1] + temp[2]
                            data = {
                                'trip_info:VendorID': str(temp[0]),
                                'trip_info:tpep_pickup_datetime': str(temp[1]),
                                'trip_info:tpep_dropoff_datetime': str(temp[2]),
                                'trip_info:passenger_count': str(temp[3]),
                                'trip_info:trip_distance': str(temp[4]),
                                'trip_info:RatecodeID': str(temp[5]),
                                'trip_info:store_and_fwd_flag': str(temp[6]),
                                'trip_info:PULocationID': str(temp[7]),
                                'trip_info:DOLocationID': str(temp[8]),
                                'trip_info:payment_type': str(temp[9]),
                                'trip_info:fare_amount': str(temp[10]),
                                'trip_info:extra': str(temp[11]),
                                'trip_info:mta_tax': str(temp[12]),
                                'trip_info:tip_amount': str(temp[13]),
                                'trip_info:tolls_amount': str(temp[14]),
                                'trip_info:improvement_surcharge': str(temp[15]),
                                'trip_info:total_amount': str(temp[16]),
                                'trip_info:congestion_surcharge': str(temp[17]),
                                'trip_info:airport_fee': str(temp[18])}
                            b.put(row_key, data)
        except Exception as error:
            logging.error("Error occurred during batch insert: %s", str(error))
        else:
            end_time = time.time()
            logging.info("Batch insert completed for %s", filename)
            logging.info("Execution time for %s: %.2f minutes\n", filename, (end_time - start_time)/60)

# Ingest the data
if __name__ == "__main__":
    st = time.time()  # Start time
    client = HBaseClient()
    client.batch_insert_data('yellow_tripdata_2017-03.csv', 'taxi_tripdata_hbase')
    client.batch_insert_data('yellow_tripdata_2017-04.csv', 'taxi_tripdata_hbase')

    elapsed_time = time.time() - st
    logging.info("Script execution time: %s", time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))