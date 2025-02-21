from first_job import Job1

if __name__ == "__main__":
    """
    Runs the first job and prints each key-value pair separated
    by space on a new line.
    """

    job = Job1()
    with job.make_runner() as runner:
        runner.run()

        for cat, count in job.parse_output(runner.cat_output()):
            print(cat, count, "\n", end="")
