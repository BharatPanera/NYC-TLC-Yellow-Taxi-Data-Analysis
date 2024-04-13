#f.How does revenue vary over time? Calculate the average trip revenue - analysing it by hour of the day (day vs night)
# Import necessary modules
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime  # Import datetime module to handle timestamp data

# Define the MRJob class for calculating average revenue by hour of the day
class MRAvgRevenueByHour(MRJob):
    
    # Mapper function to extract hour of the day and total amount
    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':  # Skip the header line
                
                # Convert pickup time stamp to a datetime object
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
                # Get the hour from the pickup datetime
                hour = putime.hour
                # Concatenate the word 'Hour' and hour of the day for output
                hour_day = 'Hour ' + str(hour)
                # Get the total amount for the trip
                amount = float(row[16])
                
                # Yield the hour of the day and total amount
                yield hour_day, amount
                
    # Reducer function to calculate the average revenue per hour of the day
    def reducer(self, hour_day, amount):       
        # Initialize variables for sum and count
        total_amount = 0
        count = 0
        
        # Iterate over amounts to calculate sum and count
        for amt in amount:
            count += 1
            total_amount += amt
        
        # Calculate the average revenue
        avg_revenue = total_amount / count if count != 0 else 0
        
        # Yield the hour of the day and its average revenue
        yield hour_day, avg_revenue

# If the script is run directly, execute the MRJob
if __name__ == '__main__':
    MRAvgRevenueByHour.run()

