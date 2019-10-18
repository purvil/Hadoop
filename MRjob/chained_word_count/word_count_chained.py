
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRWordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words, reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_flip_key_val, reducer=self.reducer_final)
        ]
    def mapper_get_words(self, _, line):
        line = re.sub('[^A-Za-z]', ' ', line)
        for word in line.split():
            yield word.lower(), 1
    def reducer_count_words(self, word, count):
        yield word, sum(count)
        
    def mapper_flip_key_val(self, word, count):
        yield '%04d'%int(count), word
        
    def reducer_final(self, count, word):
        for w in word:
            yield count, w
        
if __name__ == "__main__":
    MRWordCount.run()
