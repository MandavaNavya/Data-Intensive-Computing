from mrjob.job import MRJob
from mrjob.step import MRStep
from json import loads

class Job1(MRJob):
    def mapper_count(self, _, line):
        """
        The mapper parses the input line and extracts the category of the review.
        Input:
            key: ignored
            value: json representing the review
        Output:
            key: category
            value: 1 
        """
        review = loads(line)
        cat = review["category"]
        yield cat, 1
    
    def combiner_count(self, cat, c):
        """
        The combiner sums the list of values and yields just one value with the sum.
        """
        yield cat, sum(c)
    
    def reducer_count(self, cat, c):
        """
        The reducer sums the list of values and yields just one value with the sum.
        """
        yield cat, sum(c)


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_count,
                combiner=self.combiner_count,
                reducer=self.reducer_count
            )
        ]
    
if __name__ == "__main__":
    Job1().run()
