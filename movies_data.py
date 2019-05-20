'''Creator: William Purviance   william.purviance@wsu.edu   3/21/2019
    Description: Creates a Movie Database schema and parses and inserts
    movie data from a .csv file into the database. The program has 3
    commandline arguments. <Username> <Password> and <QueryNumber>.
    Username and password access the users MySQL server, and query number
    is an option argument specifying the assignment query to run (or none,
    which runs all the queries.'''

import csv
import ast
import sys
import mysql.connector
from mysql.connector import errorcode

# Check if query is in range.
try:
    if int(sys.argv[3]) not in range(1,6):
        print("Error, invalid query input.")
        exit(1)
    else:
        query_num = int(sys.argv[3]) # Query number to perform
except IndexError:
    query_num = -1
try:
    cnx = mysql.connector.connect(user=sys.argv[1],password=sys.argv[2])
    cursor = cnx.cursor()
except:
    print("Error, invalid username/password.")
    exit(1)

def create_movies_db():
    ''' Defines movie DB relations. Note: I used the method of insertion
        shown in the mysql/connector documentation. In case there is any issues
        with the similarities in code.'''

    TABLES = {}
    TABLES['Movies'] = (
            "CREATE TABLE Movies ("
            "   budget INT,"
            "   homepage VARCHAR(255),"
            "   id INT NOT NULL,"
            "   original_language VARCHAR(4) NOT NULL,"
            "   original_title VARCHAR(255) NOT NULL,"
            "   overview TEXT,"
            "   popularity DOUBLE(14, 10),"
            "   release_date DATE,"
            "   revenue BIGINT,"
            "   runtime INT NOT NULL,"
            "   status VARCHAR(20) NOT NULL,"
            "   tagline TINYTEXT,"
            "   title TINYTEXT,"
            "   vote_average FLOAT(10, 5),"
            "   vote_count INT,"
            "   PRIMARY KEY(id))")
    
    TABLES['Genres'] = (
            "CREATE TABLE Genres ("
            "   id INT NOT NULL,"
            "   name VARCHAR(35) NOT NULL,"
            "   PRIMARY KEY(id))")

    TABLES['MovieGenre'] = (
            "CREATE TABLE MovieGenre ("
            "   id INT NOT NULL AUTO_INCREMENT,"
            "   genreId INT NOT NULL,"
            "   movieId INT NOT NULL,"
            "   PRIMARY KEY(id),"
            "   FOREIGN KEY(genreId) REFERENCES Genres(id),"
            "   FOREIGN KEY(movieId) REFERENCES Movies(id))")

    TABLES['Keywords'] = (
            "CREATE TABLE Keywords("
            "   id INT NOT NULL,"
            "   name VARCHAR(50) NOT NULL,"
            "   PRIMARY KEY(id))")

    TABLES['MovieKeywords'] = (
            "CREATE TABLE MovieKeywords("
            "   id INT NOT NULL AUTO_INCREMENT,"
            "   keywordId INT NOT NULL,"
            "   movieId INT NOT NULL,"
            "   PRIMARY KEY(id),"
            "   FOREIGN KEY(keywordId) REFERENCES Keywords(id),"
            "   FOREIGN KEY(movieId) REFERENCES Movies(id))")
    
    TABLES['ProductionCompanies'] = (
            "CREATE TABLE ProductionCompanies("
            "   id INT NOT NULL,"
            "   name VARCHAR(50),"
            "   PRIMARY KEY(id))")

    TABLES['MovieProducer'] = (
            "CREATE TABLE MovieProducer("
            "   id INT NOT NULL AUTO_INCREMENT,"
            "   producerId INT NOT NULL,"
            "   movieId INT NOT NULL,"
            "   PRIMARY KEY(id),"
            "   FOREIGN KEY(producerId) REFERENCES ProductionCompanies(id),"
            "   FOREIGN KEY(movieId) REFERENCES Movies(id))")

    TABLES['ProductionCountries'] = (
            "CREATE TABLE ProductionCountries("
            "   iso_3166_1 VARCHAR(2) NOT NULL,"
            "   name VARCHAR(50) NOT NULL,"
            "   PRIMARY KEY(iso_3166_1))")

    TABLES['MovieCountry'] = (
            "CREATE TABLE MovieCountry("
            "   id INT NOT NULL AUTO_INCREMENT,"
            "   iso VARCHAR(2) NOT NULL,"
            "   movieId INT NOT NULL,"
            "   PRIMARY KEY(id),"
            "   FOREIGN KEY(iso) REFERENCES ProductionCountries(iso_3166_1),"
            "   FOREIGN KEY(movieId) REFERENCES Movies(id))")

    TABLES['SpokenLanguages'] = (
            "CREATE TABLE SpokenLanguages("
            "   iso_639_1 VARCHAR(2) NOT NULL,"
            "   name VARCHAR(25),"
            "   PRIMARY KEY(iso_639_1))")

    TABLES['MovieLanguage'] = (
            "CREATE TABLE MovieLanguage("
            "   id INT NOT NULL AUTO_INCREMENT,"
            "   iso VARCHAR(2) NOT NULL,"
            "   movieId INT NOT NULL,"
            "   PRIMARY KEY(id),"
            "   FOREIGN KEY(iso) REFERENCES SpokenLanguages(iso_639_1),"
            "   FOREIGN KEY(movieId) REFERENCES Movies(id))")
    
    # Check if database exists, and set it accordingly in the connection.
    try:
        cursor.execute("USE MoviesDB")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            try:
                cursor.execute("CREATE DATABASE MoviesDB DEFAULT CHARACTER SET 'utf8'")
            except mysql.connector.Error as err:
                print("Error creating database")
                exit(1)
            cnx.database = "MoviesDB"
        else:
            print(err)
            exit(1)
    
    # Create tables.
    for table_name in TABLES:
        create_table = TABLES[table_name]
        try:
            #print("Creating table {}: ".format(table_name), end='')
            cursor.execute(create_table)
        except mysql.connector.Error as err:
            #if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                #print("Table exists already")
            pass

def insert_movies():
    '''Parses .csv file of the movies schema then inserts into MoviesDB'''

    with open('tmdb_5000_movies.csv', 'r') as csv_file:
        movies_reader = csv.reader(csv_file)
        insertMovie = ("INSERT INTO Movies "
                    "(budget, homepage, id, original_language, original_title, \
                            overview, popularity, release_date, revenue, runtime, \
                            status, tagline, title, vote_average, vote_count)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        insertGenre = ("INSERT INTO Genres "
                    "(id, name)"
                    "VALUES (%s, %s)")

        insertMovieGenre = ("INSERT INTO MovieGenre "
                            "(genreId, movieId)"
                            "VALUES (%s, %s)")

        insert_keywords = ("INSERT INTO Keywords "
                        "(id, name)"
                        "VALUES (%s, %s)")
        
        insert_MovieKeywords = ("INSERT INTO MovieKeywords "
                                "(keywordId, movieId)"
                                "VALUES (%s, %s)")

        insert_ProductionCompany = ("INSERT INTO ProductionCompanies "
                                    "(id, name)"
                                    "VALUES (%s, %s)")

        insert_MovieProducer = ("INSERT INTO MovieProducer "
                                "(producerId, movieId)"
                                "VALUES (%s, %s)")

        insert_ProductionCountry = ("INSERT INTO ProductionCountries "
                                    "(iso_3166_1, name)"
                                    "VALUES (%s, %s)")

        insert_MovieCountry = ("INSERT INTO MovieCountry "
                            "(iso, movieId)"
                            "VALUES (%s, %s)")

        insert_SpokenLanguages = ("INSERT  INTO SpokenLanguages "
                                "(iso_639_1, name)"
                                "VALUES (%s, %s)")

        insert_MovieLanguage = ("INSERT INTO MovieLanguage "
                                "(iso, movieId)"
                                "VALUES (%s, %s)")

        attri_values = next(movies_reader)
        for row in movies_reader:
            rows = (row[0], row[2], row[3], row[5], row[6], row[7], row[8], row[11],\
                    row[12], row[13], row[15], row[16], row[17], row[18], row[19])
            # Incase multi-valued rows contains invalid attribute data. I put this in a 'try' statement.
            try:
                genres = ast.literal_eval(row[1])
                keyword_attri = ast.literal_eval(row[4])
                company_attri = ast.literal_eval(row[9])
                country_attri = ast.literal_eval(row[10])
                language_attri = ast.literal_eval(row[14])
            except:
                continue

            try:
                cursor.execute(insertMovie, rows)
            except mysql.connector.Error as err:
                #print(err.msg)
                continue

            # Insert into genre and genre relation table.
            for key in genres:
                movie_genre_values = (key['id'], row[3])
                id_name = (key['id'], key['name'])
                try:
                    cursor.execute(insertGenre, id_name)
                except mysql.connector.Error as error:
                    pass
                try:
                    cursor.execute(insertMovieGenre, movie_genre_values)
                except mysql.connector.Error as error:
                    pass
            # Insert into keyword and keyword relation table.
            for key in keyword_attri:
                keyword_values = (key['id'], key['name'])
                MovieKeywords_values = (key['id'], row[3])
                try:
                    cursor.execute(insert_keywords, keyword_values)
                except mysql.connector.Error as error:
                    pass
                try:
                    cursor.execute(insert_MovieKeywords, MovieKeywords_values)
                except mysql.connector.Error as error:
                    pass
            # Insert into producer and producer relation table.  
            for key in company_attri:
                company_values = (key['id'], key['name'])
                MovieProducer_values = (key['id'], row[3])
                try:
                    cursor.execute(insert_ProductionCompany, company_values)
                except mysql.connector.Error as error:
                    pass
                try:
                    cursor.execute(insert_MovieProducer, MovieProducer_values)
                except mysql.connector.Error as error:
                    pass
            # Insert into country and country relation table.
            for key in country_attri:
                country_values = (key['iso_3166_1'], key['name'])
                MovieCountry_values = (key['iso_3166_1'], row[3])
                try:
                    cursor.execute(insert_ProductionCountry, country_values)
                except mysql.connector.Error as error:
                    pass
                try:
                    cursor.execute(insert_MovieCountry, MovieCountry_values)
                except mysql.connector.Error as error:
                    pass
            # Insert into language and language relation table.
            for key in language_attri:
                language_values = (key['iso_639_1'], key['name'])
                MovieLanguage_values = (key['iso_639_1'], row[3])
                try:
                    cursor.execute(insert_SpokenLanguages, language_values)
                except:
                    pass
                try:
                    cursor.execute(insert_MovieLanguage, MovieLanguage_values)
                except:
                    pass

    cnx.commit()

def query_1():
    '''Average budget of all movies, sql query'''
    query1 = ("SELECT avg(budget) "
              "FROM Movies")

    cursor.execute(query1)
    for avg in cursor:
        print("\nAverage budget of all movies:\n" + str(avg[0]) + '\n')

def query_2():
    '''Movies produced in US, sql query'''
    query2 = ("SELECT original_title, ProductionCompanies.name "
              "FROM Movies "
              "JOIN MovieProducer ON MovieProducer.movieId=Movies.id "
              "JOIN ProductionCompanies ON MovieProducer.producerId=ProductionCompanies.id "
              "JOIN MovieCountry ON MovieCountry.movieId=Movies.id "
              "WHERE MovieCountry.iso='US' "
              "LIMIT 5")

    cursor.execute(query2)
    print("Movies Produced in the US:")
    print("Movie Title  /   Production Company")
    for title, company in cursor:
        print(title + "    |    " + company)
    print()


def query_3():
    '''Top 5 revenue, sql query'''
    query3 = ("SELECT Movies.original_title, revenue "
              "FROM Movies "
              "ORDER BY revenue DESC "
              "LIMIT 5")

    cursor.execute(query3)
    print("Top 5 Revenue:")
    print("Movie Title  /   Revenue")
    for title, revenue in cursor:
        print(title + "    |    " + str(revenue))
    print()

def query_4():
    '''Science Fiction and Myserty movies, sql query'''
    query4 = ("SELECT Movies.original_title, Genres.name "
              "FROM Movies "
              "JOIN MovieGenre ON Movies.id=MovieGenre.movieId "
              "JOIN Genres ON MovieGenre.genreId=Genres.id "
              "WHERE MovieGenre.movieId IN (SELECT MovieGenre.movieId "
                  "FROM MovieGenre "
	          "JOIN Genres ON MovieGenre.genreId=Genres.id "
	          "WHERE Genres.id=878 AND MovieGenre.movieId IN (SELECT MovieGenre.MovieId "
		      "FROM MovieGenre "
                      "JOIN Genres ON MovieGenre.genreId=Genres.id "
                      "WHERE Genres.id=9648)) "
              "LIMIT 5")

    cursor.execute(query4)
    print("Science Fiction Mystery Movies:")
    print("Movie title  /   Genres")
    for title, name in cursor:
        print(title + "    |    " + name)
    print()

def query_5():
    '''Movies more popular than the mean film popularity, sql query.'''
    query5 = ("SELECT Movies.original_title, Movies.popularity "
              "FROM Movies "
              "WHERE Movies.popularity > (SELECT avg(Movies.popularity) "
	          "FROM Movies) "
              "LIMIT 5")
    
    cursor.execute(query5)
    print("Higher than mean popularity:")
    print("Movie Title  /   Popularity")
    for title, pop in cursor:
        print(title + "    |    " + str(pop))
    print()

create_movies_db()
insert_movies()

# If query specified do that query number, else perform all.
if query_num == 1:
    query_1()
elif query_num == 2:
    query_2()
elif query_num == 3:
    query_3()
elif query_num == 4:
    query_4()
elif query_num == 5:
    query_5()
else:
    query_1()
    query_2()
    query_3()
    query_4()
    query_5()

# I tried to find a more clever way to do the above, but alas, failed.

cursor.close()
cnx.close()
