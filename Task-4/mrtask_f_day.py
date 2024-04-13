#f.How does revenue vary over time? Calculate the average trip revenue per month - analysing it by hour of the day (day vs night)
# Import necessary modules
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime  # Import datetime module to handle timestamp data

# Define the MRJob class for calculating average revenue by day of the week
class MRAvgRevenueByDay(MRJob):

    # Mapper function to extract day of the week and total amount
    def mapper(self, _, line):
        # Dictionary to map integers to weekdays
        days = {1:'Monday', 2:'Tuesday', 3:'Wednesday', 4:'Thursday', 5:'Friday', 6:'Saturday', 7:'Sunday'}
        
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':  # Skip the header line
                
                # Convert pickup time stamp to a datetime object
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
                # Get the day of the week as an integer (1-7)
                weekday = putime.isoweekday()
                # Get the corresponding weekday name
                dayofweek = days.get(weekday)
                
                # Get the total amount for the trip
                amount = float(row[16])
                
                # Yield the day of the week and total amount
                yield dayofweek, amount

    # Reducer function to calculate the average revenue per day of the week
    def reducer(self, day, amount):
        # Initialize variables for sum and count
        total_amount = 0
        count = 0
        
        # Iterate over amounts to calculate sum and count
        for amt in amount:
            count += 1
            total_amount += amt
        
        # Calculate the average revenue
        avg_revenue = total_amount / count if count != 0 else 0
        
        # Yield the day of the week and its average revenue
        yield day, avg_revenue

# If the script is run directly, execute the MRJob
if __name__ == '__main__':
    MRAvgRevenueByDay.run()

