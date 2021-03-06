
from mrjob.job import MRJob
import re

class MRWordCountCombiner(MRJob):
    def mapper(self, _, line):
        line = re.sub('[^A-Za-z]', ' ', line)
        for word in line.split():
            yield word.lower(), 1
    def reducer(self, word, count):
        yield word, sum(count)
        
    def combiner(self, word, count):
        yield word, sum(count)
if __name__ == "__main__":
    MRWordCountCombiner.run()
