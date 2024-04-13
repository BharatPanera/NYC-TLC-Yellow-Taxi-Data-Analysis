#d. What is the average trip time for different pickup locations?
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MRAverageTripTime(MRJob):

    def steps(self):
        # Define the steps for the MapReduce job
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
        ]

    def mapper(self, _, line):
        # Mapper function: Extracts pickup location and calculates trip time
        row = line.split(',')
        if len(row) >= 8:
            if row[0] != 'VendorID':  
                # Strip any extra characters or whitespace from date strings
                pickup_time = datetime.strptime(row[1].strip(), '%Y-%m-%d %H:%M:%S')
                drop_time = datetime.strptime(row[2].strip(), '%Y-%m-%d %H:%M:%S')
                
                # Calculate trip time in seconds
                trip_time = (drop_time - pickup_time).total_seconds()
                pickup_location = row[7]
                
                # Yield pickup location and trip time
                yield pickup_location, trip_time

    def reducer(self, loc, time):
        # Reducer function: Calculates average trip time for each location
        total_time = 0
        count = 0
        # Iterate over the trip times for each location
        for t in time:
            total_time += t
            count += 1
        # Calculate average trip time
        average_time = int(total_time / count) if count > 0 else 0  # Avoid division by zero
        
        #Converting average duration into Hours, Minutes and Seconds
        hours = average_time // 3600
        minutes = (average_time % 3600) // 60
        seconds = average_time % 60

        result = str(hours) + 'hours ' + str(minutes) + 'minutes ' + str(seconds) + 'seconds'

        # Yield location and average trip time
        yield (loc, result)

if __name__ == '__main__':
    # Run the MapReduce job
    MRAverageTripTime.run()

