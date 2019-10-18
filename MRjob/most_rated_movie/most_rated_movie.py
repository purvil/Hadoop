
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_movie_review_count),
            MRStep(reducer=self.reducer_find_max)
        ]
    
    def mapper_get_ratings(self, _, line):
        uid, mid, rating, time = line.split('\t')
        yield mid, 1
    
    def reducer_movie_review_count(self, movie ,count):
        yield None, (sum(count), movie)
    
    def reducer_find_max(self, _, values):
        yield max(values)
        
if __name__ == "__main__":
    MostRatedMovie.run()
