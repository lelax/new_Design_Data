import pandas as pd
from pandas import read_csv
from pandas import read_json
from pandas import read_sql
from pandas import merge
from pandas import concat
from pandas import Series
import sqlite3
import json
from sqlite3 import connect


# class for Identifiable Entity
class IdentifiableEntity(object):

    def __init__(self, id):
        self.id = id
        self.id_array = set()
        for identifier in id:
            self.id_array.add(identifier)

    # methods of identifiableEntity

    def getIds(self):
        result = []
        for identifier in self.id_array:
            result.append(identifier)
        result.sort()
        return result

    def addId(self, id):
        result = True
        if id not in self.id:
            self.id.add(id)
        else:
            result = False
        return result

    def removeId(self, identifier):
        result = True
        if identifier in self.id:
            self.id.remove(identifier)
        else:
            result = False
        return result


# class for Person
class Person(IdentifiableEntity):

    def __init__(self, id, givenName, familyName):
        super().__init__(id)
        self.givenName = givenName
        self.familyName = familyName

    # methods of person
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


# class for publication
class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set(author)
        self.cites = set(cites)

        super().__init__(id)

    # methods of publications
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    def getCitedPublications(self):
        self.cites = []
        for p in self.cites:
            self.id.add(p)

        return self.cites

    def getAuthors(self):
        self.author = []
        for p in self.author:
            self.author.add(p)

        return self.author


# class for journal article
class JournalArticle(Publication):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(id, publicationYear, title, publicationVenue, author, cites)

    # methods of journal article
    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume


# class for book chapter
class BookChapter(Publication):
    def __init__(self, id, publicationYear, title, publicationVenue, author, cites, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, publicationYear, title, publicationVenue, author, cites)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass


# class for venue
class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title
        self.publisher = set(publisher)
        super().__init__(id)

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher


class Journal(Venue):
    pass


class Book(Venue):
    pass


# class for proceedings
class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event
        super().__init__(id, title, publisher)

    def getEvent(self):
        return self.event


# class for organization
class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class QueryProcessor(object):
    def __init__(self):
        pass


# classes for the processors
class RelationalProcessor(object):

    def __init__(self, dbPath=None):
        self.dbPath = dbPath  # dbPath: the variable containing the path of the database,
        # initially set as an empty string, that will be updated with the method setDbPath.

    # Methods
    def getDbPath(self):  # it returns the path of the database.
        return self.dbPath

    def setDbPath(self, path):
        self.dbPath = path


class RelationalDataProcessor(RelationalProcessor):

    def __init__(self, dbPath):
        super().__init__(dbPath)
        self.path = None

    def uploadData(self, path):
        self.path = path
        result = True

        while True:
            try:

                if self.path.endswith('csv'):
                    with open(path, "r", encoding="utf-8") as file:
                        publications = pd.read_csv(file, keep_default_na=False,
                                                   dtype={
                                                       "id": "string",
                                                       "title": "string",
                                                       "type": "string",
                                                       "publication_year": "int",
                                                       "issue": "string",
                                                       "volume": "string",
                                                       "chapter": "string",
                                                       "publication_venue": "string",
                                                       "venue_type": "string",
                                                       "publisher": "string",
                                                       "event": "string"
                                                   })

                        journal_article = pd.DataFrame({
                            "internalId", "issue", "volume", "publication_year", "title", "publication_venue", "id"})

                        book_chapter = pd.DataFrame({
                            "internalId", "chapter_number", "publication_year", "title", "publication_venue", "id"})

                        proceedings_paper = pd.DataFrame({
                            "internalId", "publication_year", "title", "publication_venue", "id"})

                        proceedings = pd.DataFrame({
                            "internalId", "title", "event", "id"})

                        publications = publications.drop_duplicates()

                        pub_ids = pd.DataFrame(publications['id'])

                        publications_internal_id = []

                        for idx, row in pub_ids.iterrows():
                            publications_internal_id.append("publications-" + str(idx))

                        publications['internalId'] = pd.Series(publications_internal_id)

                        # Data Frame for publications

                        publications_df = pd.DataFrame(
                            {'internalId', 'doi', 'title', 'publication_year', 'publication_venue'})
                        publications_df['internalId'] = publications['internalId']
                        publications_df['doi'] = publications['id'].astype('str')
                        publications_df['title'] = publications['title'].astype('str')
                        publications_df['type'] = publications['type'].astype('str')
                        publications_df['publication_year'] = publications['publication_year'].astype('int')
                        publications_df['publication_venue'] = publications['publication_venue'].astype('str')

                        # Data Frame for journal article

                        journal_article['internalId'] = publications[publications['type'] == "journal-article"][
                            'internalId'].astype('str')
                        journal_article['doi'] = publications[publications['type'] == "journal-article"]['id'].astype(
                            'str')
                        journal_article['issue'] = publications[publications['type'] == "journal-article"][
                            'issue'].astype(
                            'str')
                        journal_article['volume'] = publications[publications['type'] == "journal-article"][
                            'volume'].astype(
                            'str')

                        # Data Frame for book chapter

                        book_chapter['internalId'] = publications[publications['type'] == "book-chapter"][
                            'internalId'].astype('str')
                        book_chapter['doi'] = publications[publications['type'] == "book-chapter"]['id'].astype('str')
                        book_chapter['chapter'] = publications[publications['type'] == "book-chapter"][
                            'chapter'].astype(
                            'str')

                        # Data Frame for Proceedings

                        proceedings = pd.DataFrame({
                            "internalId", "title", "publisher", "event"})

                        proceedings['internalId'] = publications['internalId'].astype('str')
                        proceedings['doi'] = publications['id'].astype('str')
                        proceedings['title'] = publications['title'].astype('str')
                        proceedings['publisher'] = publications['publisher'].astype('str')
                        proceedings['event'] = publications['event'].astype('str')
                        proceedings['publication_venue'] = publications['publication_venue'].astype('str')

                        # Data Frame for Proceedings Paper

                        proceedings_paper['internalId'] = publications['internalId'].astype('str')
                        proceedings_paper['doi'] = publications['id'].astype('str')

                    with connect(self.dbPath) as con:
                        publications_df.to_sql("Publications", con, if_exists="append", index=False)
                        journal_article.to_sql("JournalArticle", con, if_exists="append", index=False)
                        book_chapter.to_sql("BookChapter", con, if_exists="append", index=False)
                        proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="append", index=False)

                    con.commit()


                elif self.path.endswith('.json'):

                    with open(path, "r", encoding="utf-8") as file:
                        venue = json.load(file)

                        journal = pd.DataFrame({
                            "internalId", "title", "id"})

                        book = pd.DataFrame({
                            "internalId", "title", "id"})

                        proceedings = pd.DataFrame({
                            "internalId", "title", "publisher", "event"})

                        # DataFrame for authors being populated
                        authors_df = pd.DataFrame({
                            "doi_authors": pd.Series(dtype="str"),
                            "family": pd.Series(dtype="str"),
                            "given": pd.Series(dtype="str"),
                            "orcid": pd.Series(dtype="str")
                        })

                        family = []
                        given = []
                        orcid = []
                        doi_authors = []

                        authors = venue['authors']
                        for key in authors:
                            for value in authors[key]:
                                doi_authors.append(key)
                                family.append(value['family'])
                                given.append(value['given'])
                                orcid.append(value['orcid'])

                        authors_df['doi_authors'] = doi_authors
                        authors_df['family'] = family
                        authors_df['given'] = given
                        authors_df['orcid'] = orcid

                        # Data Frame for internal ID Venue

                        venues_id_df = pd.DataFrame({
                            "doi_venues_id": pd.Series(dtype="str"),
                            "issn_isbn": pd.Series(dtype="str"),
                        })
                        doi_venues_id = []
                        issn_isbn = []

                        venues_id = venue["venues_id"]
                        for key in venues_id:
                            for value in venues_id[key]:
                                doi_venues_id.append(key)
                                issn_isbn.append(value)

                        venues_id_df["doi_venues_id"] = doi_venues_id
                        venues_id_df["issn_isbn"] = pd.Series(issn_isbn)

                        venue_int = []
                        for idx, row in venues_id_df.iterrows():
                            venue_int.append("venue-" + str(idx))

                        venues_id_df["internalId"] = venue_int

                        # Data Frame for references
                        references_df = pd.DataFrame({
                            "id_ref": pd.Series(dtype="str"),
                            "id_ref_doi": pd.Series(dtype="str")
                        })

                        id_ref = []
                        id_ref_doi = []

                        references = venue["references"]

                        for key in references:
                            for value in references[key]:
                                id_ref.append(key)
                                id_ref_doi.append(value)

                        references_df["id_ref"] = id_ref
                        references_df["id_ref_doi"] = id_ref_doi

                        # Data Frame for publishers

                        publishers_df = pd.DataFrame({
                            "id_pub": pd.Series(dtype="str"),
                            "name": pd.Series(dtype="str")
                        })

                        doi_pub = []
                        name = []
                        id_pub = []

                        publishers = venue["publishers"]

                        for key in publishers:
                            for value in publishers[key]:
                                doi_pub.append(key)
                                id_pub.append(publishers[key]["id"])
                                name.append(publishers[key]["name"])

                        publishers_df["doi"] = doi_pub
                        publishers_df["id_pub"] = id_pub
                        publishers_df["name"] = name
                        publishers_df["internalId"] = venues_id_df['internalId']

                        authors_df["internalId"] = venues_id_df['internalId']

                        references["internalId"] = venues_id_df['internalId']

                        # Creating the tables into the database

                    with connect(self.dbPath) as con:

                        authors_df.to_sql("Authors", con, if_exists="append", index=False)
                        venues_id_df.to_sql("Venues", con, if_exists="append", index=False)
                        references_df.to_sql("References", con, if_exists="append", index=False)
                        publishers_df.to_sql("Publishers", con, if_exists="append", index=False)

                    con.commit()

                else:
                    result = False

            except ValueError:
                print("Oops! That's not a string.")
                result = False

            return result
        
class RelationalQueryProcessor:
    def __init__(self, dbPath=None):
        self.dbPath = dbPath

    def getDbPath(self):
        return self.dbPath

    def getPublicationsPublishedInYear(self, year):
        with connect(self.getDbPath()) as con:
            query = f"""SELECT Publications.id, Publications.title, Publications.publication_venue, Publications.publication_year, Publications.type,
                            Publishers.name AS publisher_name, Authors.orcid, Authors.given, Authors.family,
                            JournalArticle.issue, JournalArticle.volume, BookChapter.chapter
                            FROM Publications
                            LEFT JOIN Authors ON Publications.id = Authors.doi_authors
                            LEFT JOIN JournalArticle ON Publications.id = JournalArticle.doi
                            LEFT JOIN BookChapter ON Publications.id = BookChapter.doi
                            LEFT JOIN Publishers ON Publications.publisher = Publishers.doi
                            WHERE Publications.publication_year ==  {year};""" # Removed quotes around {year} as it's an int
            result = pd.read_sql(query, con)
        return result

    def getPublicationsByAuthorId(self, author_orcid: str) -> pd.DataFrame:
        with connect(self.getDbPath()) as con:
            query = f"""
                SELECT
                    P.id,
                    P.title,
                    P.type,
                    P.publication_year,
                    JA.issue,
                    JA.volume,
                    BC.chapter,
                    P.publication_venue,
                    Pub.name AS publisher_name,
                    A.orcid,
                    A.given AS author_given_name,
                    A.family AS author_family_name
                FROM
                    Publications AS P
                LEFT JOIN
                    Authors AS A ON P.id = A.doi_authors
                LEFT JOIN
                    JournalArticle AS JA ON P.id = JA.doi
                LEFT JOIN
                    BookChapter AS BC ON P.id = BC.doi
                LEFT JOIN
                    Publishers AS Pub ON P.publisher = Pub.doi
                WHERE
                    A.orcid = '{author_orcid}';
            """
            result = pd.read_sql(query, con)
        return result

    def getPublicationByAuthorInputName(self, name: str) -> pd.DataFrame:
        search_name_lower = name.lower()
        with connect(self.getDbPath()) as con:
            query = f"""
                SELECT
                    P.id,
                    P.title,
                    P.type,
                    P.publication_year,
                    JA.issue,
                    JA.volume,
                    BC.chapter,
                    P.publication_venue,
                    Pub.name AS publisher_name,
                    A.orcid,
                    A.given AS author_given_name,
                    A.family AS author_family_name
                FROM
                    Publications AS P
                LEFT JOIN
                    Authors AS A ON P.id = A.doi_authors
                LEFT JOIN
                    JournalArticle AS JA ON P.id = JA.doi
                LEFT JOIN
                    BookChapter AS BC ON P.id = BC.doi
                LEFT JOIN
                    Publishers AS Pub ON P.publisher = Pub.doi
                WHERE
                    LOWER(A.family) LIKE '%{search_name_lower}%'
                    OR LOWER(A.given) LIKE '%{search_name_lower}%';
            """
            result = pd.read_sql(query, con)
        return result


def setup_test_database(db_path=":memory:"):
    """
    Sets up an in-memory SQLite database with a schema mimicking your
    RelationalDataProcessor's output, and populates it with test data.
    """
    con = connect(db_path)
    cursor = con.cursor()

    # Create Publications table
    cursor.execute("""
        CREATE TABLE Publications (
            id TEXT PRIMARY KEY,
            title TEXT,
            type TEXT,
            publication_year INTEGER,
            issue TEXT,
            volume TEXT,
            chapter TEXT,
            publication_venue TEXT,
            venue_type TEXT,
            publisher TEXT, -- This stores the publisher's DOI/ID (e.g., crossref:1)
            event TEXT
        );
    """)

    # Create Authors table
    cursor.execute("""
        CREATE TABLE Authors (
            orcid TEXT,
            given TEXT,
            family TEXT,
            doi_authors TEXT, -- Foreign key to Publications.id
            PRIMARY KEY (orcid, doi_authors)
        );
    """)

    # Create Publishers table
    cursor.execute("""
        CREATE TABLE Publishers (
            doi TEXT PRIMARY KEY, -- This is the ID stored in Publications.publisher
            id_pub TEXT,          -- Redundant, but matching your uploadData
            name TEXT
        );
    """)

    # Create JournalArticle table
    cursor.execute("""
        CREATE TABLE JournalArticle (
            internalId TEXT PRIMARY KEY,
            doi TEXT, -- Foreign key to Publications.id
            issue TEXT,
            volume TEXT
        );
    """)

    # Create BookChapter table
    cursor.execute("""
        CREATE TABLE BookChapter (
            internalId TEXT PRIMARY KEY,
            doi TEXT, -- Foreign key to Publications.id
            chapter TEXT
        );
    """)

    # Create ProceedingsPaper table (not used in current queries, but for completeness)
    cursor.execute("""
        CREATE TABLE ProceedingsPaper (
            internalId TEXT PRIMARY KEY,
            doi TEXT -- Foreign key to Publications.id
        );
    """)

    # Create Venues table (based on your JSON parsing, though its role is still a bit ambiguous)
    cursor.execute("""
        CREATE TABLE Venues (
            doi_venues_id TEXT PRIMARY KEY, -- e.g., an ISSN/ISBN or venue-specific ID
            issn_isbn TEXT,                 -- Redundant, but matching your uploadData
            internalId TEXT
        );
    """)


    # --- Populate with Test Data ---

    # Publications Data
    publications_data = [
        ('doi:pub1', 'The Art of Python', 'journal-article', 2023, '1', '10', '', 'Python Journal', 'journal', 'crossref:pubA', ''),
        ('doi:pub2', 'SQL for Beginners', 'book-chapter', 2022, '', '', '5', 'Database Basics', 'book', 'crossref:pubB', ''),
        ('doi:pub3', 'Advanced ML', 'journal-article', 2023, '2', '15', '', 'AI Review', 'journal', 'crossref:pubA', ''),
        ('doi:pub4', 'Data Science Insights', 'proceedings-paper', 2024, '', '', '', 'Conf Proceedings 2024', 'proceedings', 'crossref:pubC', 'DataCon 2024'),
        ('doi:pub5', 'Joanna\'s New Paper', 'journal-article', 2023, '3', '12', '', 'Sci Reports', 'journal', 'crossref:pubB', ''),
        ('doi:pub6', 'The Smith Saga', 'book-chapter', 2021, '', '', '2', 'Modern History', 'book', 'crossref:pubC', ''),
        ('doi:pub7', 'A Study on Jo', 'journal-article', 2025, '1', '1', '', 'New Discoveries', 'journal', 'crossref:pubA', '')
    ]
    cursor.executemany("INSERT INTO Publications VALUES (?,?,?,?,?,?,?,?,?,?,?)", publications_data)

    # Authors Data
    authors_data = [
        ('orcid:0001', 'Alice', 'Smith', 'doi:pub1'),
        ('orcid:0002', 'Bob', 'Johnson', 'doi:pub1'),
        ('orcid:0003', 'Charlie', 'Brown', 'doi:pub2'),
        ('orcid:0004', 'David', 'Davis', 'doi:pub3'),
        ('orcid:0005', 'Emily', 'Clark', 'doi:pub3'),
        ('orcid:0006', 'Frank', 'White', 'doi:pub4'),
        ('orcid:0007', 'Joanna', 'Blake', 'doi:pub5'), # For 'Joanna' search
        ('orcid:0008', 'John', 'Smith', 'doi:pub6'),    # For 'John' and 'Smith' search
        ('orcid:0009', 'Joseph', 'Black', 'doi:pub7')   # For 'Jo' search
    ]
    cursor.executemany("INSERT INTO Authors VALUES (?,?,?,?)", authors_data)

    # Publishers Data
    publishers_data = [
        ('crossref:pubA', 'id_pubA', 'Academic Press'),
        ('crossref:pubB', 'id_pubB', 'Science Books Ltd.'),
        ('crossref:pubC', 'id_pubC', 'Conference Publishers')
    ]
    cursor.executemany("INSERT INTO Publishers VALUES (?,?,?)", publishers_data)

    # JournalArticle Data (linking to Publications)
    journal_article_data = [
        ('ja_int1', 'doi:pub1', '1', '10'),
        ('ja_int2', 'doi:pub3', '2', '15'),
        ('ja_int3', 'doi:pub5', '3', '12'),
        ('ja_int4', 'doi:pub7', '1', '1')
    ]
    cursor.executemany("INSERT INTO JournalArticle VALUES (?,?,?,?)", journal_article_data)

    # BookChapter Data (linking to Publications)
    book_chapter_data = [
        ('bc_int1', 'doi:pub2', '5'),
        ('bc_int2', 'doi:pub6', '2')
    ]
    cursor.executemany("INSERT INTO BookChapter VALUES (?,?,?)", book_chapter_data)

    # Venues Data (minimal, just for the join to work)
    venues_data = [
        ('Python Journal', 'ISSN:1234-5678', 'venue-0'),
        ('Database Basics', 'ISBN:978-1-23-456789-0', 'venue-1'),
        ('AI Review', 'ISSN:8765-4321', 'venue-2'),
        ('Conf Proceedings 2024', 'ISBN:978-9-87-654321-0', 'venue-3'),
        ('Sci Reports', 'ISSN:1111-2222', 'venue-4'),
        ('Modern History', 'ISBN:978-0-12-345678-9', 'venue-5'),
        ('New Discoveries', 'ISSN:9999-8888', 'venue-6')
    ]
    cursor.executemany("INSERT INTO Venues VALUES (?,?,?)", venues_data)


    con.commit()
    con.close()

    return db_path # Return the path if it's not in-memory