William Purviance	william.purviance@wsu.edu	3/21/2019

movies_data.py logs the user into their MySQL server and creates a movie database from data
contained in a movie database .csv file. It creates a 'MoviesDB' schema, then parses and
inserts data from the supplied .csv file accordingly. There are five query methods, all of
which are ran, unless the user specifies the query number.

To run movies_data.py, enter on the commandline in the form:
$python3 movies_data.py Username Password <Query>

The module attempts to login to the server with the supplied username and password.
The query parameter is optional.

Included files:

movies_data.py
