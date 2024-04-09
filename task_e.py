from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgTipstoRevenue(MRJob):

    def mapper(self, _, line):
        #skiptheheader
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7]
            total_revenue = float(fields[16])
            tips = float(fields[13])
            yield pickup_location, (tips, total_revenue)
    
    def combiner(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)
    
    def reducer(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        avg_tips_to_revenue_ratio = total_tips/total_revenue
        yield pickup_location, avg_tips_to_revenue_ratio

    if __name__ == '__main__':
        AvgTipstoRevenue.run()