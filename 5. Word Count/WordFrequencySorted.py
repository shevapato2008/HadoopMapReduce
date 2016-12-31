from mrjob.job import MRJob
from mrjob.step import MRStep           # introduce step package to deal with chaining of mappers and reducers
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words,
                   reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")      # avoids issues in mrjob 5.0
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word                           # group words by count and sort count

    def reducer_output_words(self, count, words):
        for word in words:                                      # output one word for each line
            yield count, word

if __name__ == '__main__':
    MRWordFrequencyCount.run()