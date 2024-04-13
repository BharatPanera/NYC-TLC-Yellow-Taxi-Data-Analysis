from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMaxRevenueLocation(MRJob):
    """MRJob class to find the pickup location that generates the most revenue."""

    def steps(self):
        """Define the MRJob steps."""
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum_revenue),
            MRStep(reducer=self.reducer_max_revenue_location)
        ]

    def mapper(self, _, line):
        """Mapper function to extract pickup location ID and total revenue from each line."""
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':  # Skip Header Line
                pickup_location = row[7]  # Get Pickup Location ID
                amount = float(row[16])   # Get Total Amount
                yield pickup_location, amount  # Emit Pickup Location ID and Total Amount

    def reducer_sum_revenue(self, pickup_location, amount):
        """Reducer function to calculate the total revenue for each pickup location."""
        total_revenue = sum(amount)  # Calculate sum of Total Amount for each Pickup Location
        yield None, (total_revenue, pickup_location)  # Emit Total Revenue and Pickup Location

    def reducer_max_revenue_location(self, _, revenue_location_pairs):
        """Reducer function to find the pickup location with the highest revenue."""
        max_revenue_location_pair = max(revenue_location_pairs)
        # Emit Pickup Location with highest revenue and the revenue amount
        yield max_revenue_location_pair[1], max_revenue_location_pair[0]

if __name__ == '__main__':
    MRMaxRevenueLocation.run()
