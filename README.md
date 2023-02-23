# ExerciseNotes

## Setup.

You should have received this exercise as as a tarball named jfurr_hartree.tar.gz

> tar xvfz jfurr_hartree.tar.gz

Navigate to the hartree directory and setup a virtualenv with python3

> python3 -m venv virtualenv

Active the virtualenv
> source virtualenv/bin/activate

Install python requirements
> pip install -r requirements.txt


### Setup Notes
You may also need to install rust and java if they are not already installed on your local machine

### Questions/Assumptions.

I was a little unclear about the instruction to 
"Also create new record to add total for each of legal entity, counterparty & tier."

If this was an in person interview I would have asked questions to ensure I was doing the right thing.
As a means of completing the exercise I ended up adding an extra field to the output 
(total_members_in_group) that provided a count for each group (basically a count(*) in SQL parlance).
This was used in both the pandas and apache beam approaches.


## Approach 1 - Using the pandas library

The results of the pandas solution are saved in the file: **pandas_results.csv**

To regenerate the results you can run

> python ./pandas_solution.py

The pandas code contains NOTE about the above point of confusion.  

## Approach 2 - Using the apache beam library

The results of the apche beam solution are saved in the file: **apache_beam_results.csv**

To regenerate the results you can run
> python ./apache_beam_solution.py

### Notes about the approach
This was my first time using the apache beam library.  After some initial reading I decided to go
with a schema approach.  This meant the majority of computation was done with an SqlTransform. 
This actually took quite a while to get correct because the java docker
image was slow to load and the SQL calculation took awhile to run. 

Because of this I actually sketched this out as a plain old SQL approach using Postgres and then back ported 
into the SQLTransform. (psql_version.py included)  This wasn't as straight forward as it would seem and I had to rename
some colums to make it work.
I also used an ORDER BY which as I learned isn't normaly used as part of beam.  I choose to use leave it in so as to ensure the results
where the same as the pandas library.

I feel like there might be a more 'beam' like approach, but again this my first time using the library.
If this isn't the approach the interviewer would have taken I'd for sure like more.   

## Approach 3 - Postgres (used to workout SQLTransform for apache beam)

There is no output file for this approach.  

to run it you will need a local postgres docker image.
Since this was not part of the exercise I'm not providing further info
