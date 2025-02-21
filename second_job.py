from mrjob.job import MRJob
from mrjob.step import MRStep
from json import loads
import re

class Job2(MRJob):
    FILES = ["stopwords.txt", "cat_counts.txt"]

    def mapper_words_init(self):
        """
        Initializes the mapper with two class attributes:
            regex - contains the compiled regex used for tokenization
            stopwords - set of words read from the provided file used for filtering
        """
        self.regex = re.compile(r"[\s\t\d\[\]\{\}().!?,;:+=\-_\"'`~#@&*%€$§\\/]+")
        
        with open("stopwords.txt", "r") as f:
            self.stopwords = set(map(lambda line: line.strip(), f.readlines()))
    
    def mapper_words(self, _, line):
        """
        The mapper parses the input line and extracts the category and text from the review.
        Then it carries out the tokenization, case folding and stopword filtering on the review text.
        Finally, it yields a tuple per every unique word extracted. 
        Input:
            key: ignored
            value: line from the text file containing a json representing a review
        Output:
            key: word
            value: tuple consisting of category and the value 1
        Example:
            line: {"reviewText": "Hello world, hello hadoop.", "category": "A"} 
            yields: ("hello", ("A", 1)), ("world", ("A", 1)), ("hadoop", ("A", 1)) 
        """
        review = loads(line)
        cat = review["category"]
        word_list = list(
            map(lambda word: word.lower(), self.regex.split(review["reviewText"]))
        )
        unique_words = set(
            [word for word in word_list if word not in self.stopwords and len(word) > 1]
        )

        for word in unique_words: 
            yield word, (cat, 1)
   
    def combiner_words(self, word, values):
        """
        The combiner aggregates by the categories and sums the counts, yielding just one
        value per every unique category encountered.
        Input:
            key: word
            values: list of values, where each value is a tuple like (category, 1)
        Output:
            key: word
            value: tuple consisting of the category with the number of occurrences of that category 
        Example:
            word: "hello",
            values: [("A", 1), ("B", 1), ("A", 1), ("C", 1), ("A", 1)]
            yields: ("hello", ("A", 3)), ("hello", ("B", 1)), ("hello", ("C", 1))
        """
        cat_counts = {}
        for cat, c in values:
            cat_counts[cat] = cat_counts.get(cat, 0) + c 
        for cat, c in cat_counts.items():
            yield word, (cat, c)

    def reducer_words(self, word, values):
        """
        The reducer aggregates by the categories and sums the counts within category, 
        while also counting the total sum of all the values, as this corresponds to the total
        occurrence of the word. Then it yields one tuple per every unique category encountered, 
        that carries the information of the word, its total number of occurrences, and the count
        of occurrences of the word in that category.
        Input:
            key: word
            values: list of values, where each value is a tuple like (category, 1)
        Output:
            key: category
            value: tuple consisting of the word, number of all occurrences of the word,
                   and count of occurrences within the category  
        Example:
            word: "hello",
            values: [("A", 3), ("B", 1), ("C", 1)]
            yields: ("A", ("hello", 5, 3)), ("B", ("hello", 5, 1)), ("C", ("hello", 5, 1))
        """
        all_occur = 0
        cat_counts = {}
        for cat, c in values:
            all_occur += c
            cat_counts[cat] = cat_counts.get(cat, 0) + c 
        for cat, c in cat_counts.items():
            yield cat, (word, all_occur, c)

    def calculate_chi_squared(self, n, cat_count, all_occur, a):
        """
        Calculates the chi_squared score for category and word based on the provided metrics.
        Input:
            n: total number of reviews
            cat_count: total number of reviews in category
            all_occur: total number of occurrences of word
            a: number of reviews in given category which contain the given word
        Return:
            -1 if the calculation encounters division by zero
            float chi_squared score otherwise 
        """
        c = cat_count - a
        b = all_occur - a
        d = n - cat_count - b
        denom = (a + b) * (a + c) * (b + d) * (c + d)
        if denom == 0:
            return -1
        return n * (a * d - b * c) ** 2 / denom
    
    def reducer_scores_init(self):
        """
        Reads the file cat_counts.txt (output of Job1) to obtain the counts per category. 
        Initializes the reducer with two class attributes:
            cat_counts - dictionary containing the count of reviews per each category,
                         where the key is the category name, and the value is the count
            total - number of all reviews, sum of the values of cat_counts
        """
        self.cat_counts = {}
        with open("cat_counts.txt", "r") as f:
            for line in f:
                parsed = line.strip().split(" ")
                if len(parsed) != 2:
                    continue
                cat, c = parsed[0], int(parsed[1])
                self.cat_counts[cat] = c
        self.total = sum(self.cat_counts.values())


    def reducer_scores(self, cat, values):
        """
        The reducer receives the list of all the words in the category, together with
        the intermediary calculated metrics, and calculates the chi_square of each word
        using the class method 'calculate_chi_squared', and determines the top 75 words
        and their scores by sorting all pairs in descending order.
        Input:
            key: category
            value: list of values where each is a tuple like (word, all_occur, c) 
        Output:
            key: category
            value: list of 75 tuples, each in the format (score, word)
        Example:
            cat: "A"
            values: [("hello", 5, 3), ("hi", 6, 3), ("hadoop", 4, 1))]
            yields: "A", [(100.52, "hadoop"), (50.225, "hello"), (25.45, "hi")]
        """
        scores = []
        for word, all_occur, a in values:
            score = self.calculate_chi_squared(self.total, self.cat_counts[cat], all_occur, a)
            scores.append((score, word))
        yield cat, sorted(scores, reverse=True)[:75]

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_words_init,
                mapper=self.mapper_words,
                combiner=self.combiner_words,
                reducer=self.reducer_words
            ),
            MRStep(
                reducer_init=self.reducer_scores_init,
                reducer=self.reducer_scores
            )
        ]
    
if __name__ == "__main__":
    Job2().run()