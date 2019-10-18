from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        userId, movieId, rating, timestamp = line.split('\t')
        yield rating, 1
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
        
if __name__ == '__main__':
    MRRatingCounter.run()
