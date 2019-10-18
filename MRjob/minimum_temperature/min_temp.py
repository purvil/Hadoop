
from mrjob.job import MRJob

class MRMinTemp(MRJob):
    def mapper(self, key, line):
        station, date, temp_type, temp, _, _, _, _ = line.split(',')
        if temp_type == 'TMIN':
            yield station, float(temp)
    def reducer(self, key, temp_lst):
        yield key, min(temp_lst)
        
if __name__ == "__main__":
    MRMinTemp.run()
