from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgTipstoRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.sort_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7]
            total_revenue = float(fields[16])
            tips = float(fields[13])
            yield pickup_location, (tips, total_revenue)

    def combiner(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        # Combine tips and revenues locally
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)

    def reducer(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        # Sum up tips and revenues
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        # Calculate average tips to revenue ratio
        avg_tips_to_revenue_ratio = total_tips / total_revenue
        yield None, (avg_tips_to_revenue_ratio, pickup_location)

    def sort_reducer(self, _, result_pair):
        # Reducer to display the sorted result    
        sorted_result = sorted(result_pair, reverse=True)
        for pair in sorted_result:
            yield pair[1], pair[0]

if __name__ == '__main__':
    AvgTipstoRevenue.run()

