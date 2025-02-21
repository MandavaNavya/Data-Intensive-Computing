from second_job import Job2

if __name__ == "__main__":
    """
    Runs the second job and prints the output, where each key-value pair 
    is split into category and value from which the tuples of (score, word) are extracted,
    and the desired formatting is applied. Hence one single line is printed per each category. 
    Additionally all the words across all categories are printed in alphabetic order, separated 
    by a space as the last line of the output.
    """

    job2 = Job2()
    with job2.make_runner() as runner:
        runner.run()

        words = set()
        for cat, top_word_scores in job2.parse_output(runner.cat_output()):
            chi_sq = []
            for score, word in top_word_scores:
                chi_sq.append(f"{word}:{score}")
                words.add(word)
            print(f"<{cat}>", " ".join(chi_sq), "\n", end="")
        print(*sorted(words), "\n", end="")
