#c.What are the different payment types used by customers and their count? The final results should be in a sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCountOfPaymentTypes(MRJob):
    
    def steps(self):
        # Define the steps for the MapReduce job
        return [
            MRStep(mapper=self.mapper,                   
                  reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]
           
    def mapper(self, _, line):                  
        # Mapper function: Extracts payment type and its count
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        #To Skip Header Line
                ptype  = row[9]             #Payment Type
                count  = 1                  #Assign Count=1 for each trip 
                #Return Payment Type and Count
                yield ptype,count                           
        
    def reducer1(self, ptype, count):       
        # Reducer function: Return Payment Type and the total trip count for each Payment Type
         yield None, (sum(count),ptype)
            
    def reducer2(self, _, result_pair): 
        #Reducer to display the sorted result
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]            
                                   
            
if __name__ == '__main__':
    MRCountOfPaymentTypes.run()                

