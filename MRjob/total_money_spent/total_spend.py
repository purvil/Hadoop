
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTotalSpent(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer= self.reducer),
            MRStep(mapper=self.mapper_2, reducer=self.reducer_2)
        ]
    def mapper(self, _, line):
        customer, item, amount = line.split(',')
        yield customer, float(amount)
    def reducer(self, customer, amount):
        yield customer, sum(amount)
    def mapper_2(self, customer, total):
        yield '%04.02f'%total, customer
    def reducer_2(self, total, customer):
        for name in customer:
            yield total, name
        
if __name__ == "__main__":
    MRTotalSpent.run()
