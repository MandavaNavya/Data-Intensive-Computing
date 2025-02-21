# About
This project provides mapreduce jobs to compute the chi_squared value for feature selection on a large set of text data.

# Requirements
* python 3.9
* mrjob 

# Structure
The mapreduce consists of two jobs, each comes with its custom runner:
* `first_job.py` - code for the first job, contains the `Job1` class, 
* `second_job.py` - code for the second job, contains the `Job2` class, 
* `first_runner.py` - runner for the first job, runs `Job1`, 
* `second_runner.py` - runner for the second job, runs `Job2`.

The script intended to run on the TU cluster is provided as `run.sh`.
It takes one command line argument, the path to the input file from which the chi_square should be calculated.

# Usage
Calculating the chi_squared in the hadoop environment on the TU cluster can be achieved by running one of the following commands. It might happen that the bash script has no execute rights when initially copied to the cluster, therefore we add these rights just in case.

The format of the command is

```
chmod +x ./run.sh && ./run.sh [input file]
```

For example, to run on the full reviews dataset the command is

```
chmod +x ./run.sh && ./run.sh hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json
```

It might be useful to redirect the output to a file as well, like so

```
chmod +x ./run.sh && ./run.sh hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json > out.txt
```
