
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovieName(MRJob):
    def configure_options(self):
        super(MostRatedMovieName, self).configure_options()
        self.add_file_option('--items', help='path to u.item') # provide extra file which will go to every node
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,reducer_init=self.reducer_init, reducer=self.reducer_movie_review_count),
            MRStep(reducer=self.reducer_find_max)
        ]
    
    # Reducer_init runs befroe reducer
    def reducer_init(self):
        self.movieName = {}
        with open('u.Item') as f:
            for line in f:
                fields = line.split('|')
                self.movieName[fields[0]] = fields[1]
    def mapper_get_ratings(self, _, line):
        uid, mid, rating, time = line.split('\t')
        yield mid, 1
    
    def reducer_movie_review_count(self, movie ,count):
        yield None, (sum(count), self.movieName[movie])
    
    def reducer_find_max(self, _, values):
        yield max(values)
        
if __name__ == "__main__":
    MostRatedMovieName.run()
