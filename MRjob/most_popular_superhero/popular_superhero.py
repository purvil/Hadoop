
from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularSuperHero(MRJob):
    def configure_options(self):
        super(PopularSuperHero, self).configure_options()
        self.add_file_option('--name', help='path to Marvel-names.txt')
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper_init= self.mapper_lookup, mapper=self.mapper_sort, reducer=self.reducer_max)
        ]
    
    def mapper(self, _, line):
        data = line.split()
        yield int(data[0]), len(data) - 1
    
    def reducer(self, hero_id, total):
        yield hero_id, sum(total)
        
    def mapper_lookup(self):
        self.lookup = {}
        with open('Marvel-names.txt') as f:
            for data in f:
                fields = data.split('"')
                heroId = int(fields[0])
                self.lookup[heroId] = fields[1]
        
    def mapper_sort(self, hero_id, total):
        hero_name = self.lookup[hero_id]
        yield None, (total, hero_name)
        
    def reducer_max(self, _, hero_lst):
        yield max(hero_lst)
        
if __name__ == "__main__":
    PopularSuperHero.run()
