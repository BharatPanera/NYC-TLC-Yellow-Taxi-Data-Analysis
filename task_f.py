from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class AvgTripRevenueOverTime(MRJob):

    def parse_datetime(self, datetime_str):
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for f in formats:
            try:
                return datetime.strptime(datetime_str, f)
            except ValueError:
                pass
            raise ValueError('no valid datetime format found')
    
    def mapper(self, _, line):
        #skip the header
        if not line.startswith('VendorID'):
            fields = line.split(',')
            revenue = float(fields[16])
            pickup_datetime = self.parse_datetime(fields[1])
            month = pickup_datetime.month
            hour = pickup_datetime.hour
            weekday = pickup_datetime.weekday()
            yield (month, hour, weekday), revenue
    
    def reducer(self, key, values):
        total_revenue = 0
        num_trips = 0

        for revenue in values:
            total_revenue += revenue
            num_trips += 1

        avg_revenue = total_revenue / num_trips
        
        yield key, avg_revenue
    
if __name__ == '__main__':
    AvgTripRevenueOverTime.run()
