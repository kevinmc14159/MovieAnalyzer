from mrjob.job import MRJob
from mrjob.step import MRStep 

class MovieAnalyzer(MRJob):
    """Script that finds most popular movies using Hadoop & MapReduce."""

    def steps(self):
        """Schedule map and reduce tasks."""
        return [
            MRStep (mapper = self.mapper_get_ratings,
                    reducer = self.reducer_count_ratings),
            MRStep (reducer = self.reducer_sort_output)
        ]

    def mapper_get_ratings(self, _, line):
        """Map data into key-value pair of (movieID, 1)."""
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        """Aggregate the number of times each movieID appears."""
        yield str(sum(values)).zfill(5), key

    def reducer_sort_output(self, count, movies):
        """Shuffle & sort the results."""
        for movie in movies:
            yield movie, count

if __name__ == '__main__':
    MovieAnalyzer.run()    