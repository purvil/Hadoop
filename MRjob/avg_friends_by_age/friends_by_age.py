
from mrjob.job import MRJob

class MRAverageFriend(MRJob):
    def mapper(self, key, line):
        u_id, name, age, friend_count = line.split(',') 
        yield age.strip(), int(friend_count)
    def reducer(self, key, count_lst):
        c = 0
        total = 0
        for count in count_lst:
            c += 1
            total += count
        yield key, total/c
    
if __name__ == "__main__":
    MRAverageFriend.run()
