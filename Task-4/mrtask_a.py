from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMaxTripsAndRevenue(MRJob):
    """MRJob class to find the vendors with the most trips and their total revenue."""

    def steps(self):
        """Define the MRJob steps."""
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_trips),
            MRStep(reducer=self.reducer_max_trips_revenue)
        ]

    def mapper(self, _, line):
        """Mapper function to extract vendor ID and total revenue from each line."""
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':  # Skip Header Line
                vendor = row[0]  # Get Vendor ID
                amount = float(row[16])  # Get Total Amount
                yield vendor, amount  # Emit Vendor ID and Total Amount

    def reducer_count_trips(self, vendor, amount):
        """Reducer function to count the trips and calculate the total revenue for each vendor."""
        total_revenue = 0
        trip_count = 0
        
        for amt in amount:
            trip_count += 1
            total_revenue += amt
        
        yield None, (trip_count, (vendor, total_revenue))  # Emit Count, Vendor ID, and Total Revenue

    def reducer_max_trips_revenue(self, _, vendor_revenues):
        """Reducer function to find the vendor with the most trips and their total revenue."""
        max_trip_count = 0
        max_vendor = None
        max_revenue = 0
        
        for trip_count, (vendor, total_revenue) in vendor_revenues:
            if trip_count > max_trip_count:
                max_trip_count = trip_count
                max_vendor = vendor
                max_revenue = total_revenue
        
        if max_vendor:  # Ensure max_vendor is not None
            yield max_vendor, max_revenue  # Emit Vendor ID and Total Revenue with most trips

if __name__ == '__main__':
    MRMaxTripsAndRevenue.run()

