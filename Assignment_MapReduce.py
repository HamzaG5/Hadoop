from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_construct,
                   reducer=self.reducer_order_results)
        ]

    # mapper function retrieves data from the u.data dataset and extracts the movieID and maps together with the value 1
    # this function returns a key value pair of how many ratings there for the movies in the dataset 
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # this function calculates the sum of ratings for each movie
    # returns a key value pair of the total number of ratings for each movie 
    def reducer_count_ratings(self, movieID, ratings):
        yield movieID, sum(ratings)

    # this function returns groups the total number of ratings with their movieIDs together
    # and constructs a new key value pair which is necessary for the next step in order to sort them
    def mapper_construct(self, movieID, sumRatings):
        yield None, (sumRatings, movieID)     

    # this method rearranges the results to sort the movies by their sum ratings
    def reducer_order_results(self, _, sumRatings):
        for count, movieID in sorted(sumRatings, reverse=True):
            yield int(movieID), count

if __name__ == '__main__':
    RatingsBreakdown.run()
