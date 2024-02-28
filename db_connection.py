#-------------------------------------------------------------------------
# AUTHOR: Jeremiah Garcia
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = ""
    DB_USER = ""
    DB_PASS = ""
    DB_HOST = ""
    DB_PORT = ""

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT)
        return conn

    except:
        print("Database not connected successfully")

def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into assignment2.categories (id_cat, name) Values (%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    sql = "select id_cat from assignment2.categories where name = %(docCat)s"
    cur.execute(sql, {'docCat': docCat})
    catId = cur.fetchall()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    num_chars = 0
    for char in docText:
        if char.isalpha():
            num_chars += 1

    sql = "Insert into assignment2.documents (doc, text, title, num_chars, date, category) Values (%s, %s, %s, %s, %s, %s)"
    recset = [docId, docText, docTitle, num_chars, docDate, catId[0][0]]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    punctuation = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    text_no_punct = ""
    for char in docText:
        if char not in punctuation:
            text_no_punct += char
    docTerms = text_no_punct.lower().split()

    # 3.2 For each term identified, check if the term already exists in the database
    sql = "select term from assignment2.terms"
    cur.execute(sql)
    currentTerms = cur.fetchall()

    for new_term in docTerms:
        found = False
        for saved_term in currentTerms:
    # 3.3 In case the term does not exist, insert it into the database
            if new_term == saved_term[0]:
                found = True
        if not found:
            currentTerms.append([new_term])

            num_chars = 0
            for char in new_term:
                if char.isalpha():
                    num_chars += 1

            sql = "Insert into assignment2.terms (term, num_chars) Values (%s, %s)"
            recset = [new_term, num_chars]
            cur.execute(sql, recset)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

    sql = "select term from assignment2.index where doc = %(docId)s"
    cur.execute(sql, {'docId': docId})
    currentIndex = cur.fetchall()

    for new_term in docTerms:
        term_count = 0
        for check_term in docTerms:
            if new_term == check_term:
                term_count += 1

        found = False
        for saved_term in currentIndex:
            if new_term == saved_term:
                found = True
        if not found:
            currentIndex.append(new_term)

            sql = "Insert into assignment2.index (doc, term, term_count) Values (%s, %s, %s)"
            recset = [docId, new_term, term_count]
            cur.execute(sql, recset)


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    sql = "select term from assignment2.index where doc = %(docId)s"
    cur.execute(sql, {'docId': docId})
    currentIndex = cur.fetchall()

    for index in currentIndex:
        sql = "Delete from assignment2.index where doc = %(docId)s and term = %(term)s"
        cur.execute(sql, {'docId': docId, 'term': index})

    sql = "select term from assignment2.index"
    cur.execute(sql)
    otherDocIndex = cur.fetchall()

    for index in currentIndex:
        found = False
        for otherIndex in otherDocIndex:
            if index == otherIndex:
                found = True

        if not found:
            sql = "Delete from assignment2.terms where term = %(term)s"
            cur.execute(sql, {'term': index})

    # 2 Delete the document from the database
    # --> add your Python code here
    sql = "Delete from assignment2.documents where doc = %(docId)s"
    cur.execute(sql, {'docId': docId})

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

    output = "{"
    sql = "select term from assignment2.terms order by term"
    cur.execute(sql)
    terms = cur.fetchall()

    for i, term in enumerate(terms):
        output += f"\'{term[0]}\':\'"
        sql = "select * from assignment2.index where term = %(term)s order by doc"
        cur.execute(sql, {'term': term[0]})
        index = cur.fetchall()

        for j, index_term in enumerate(index):
            sql = "select title from assignment2.documents where doc = %(docId)s"
            cur.execute(sql, {'docId': index_term[0]})
            documentTitle = cur.fetchall()
            if len(index) != j + 1 and len(index) != 1:
                output += f"{documentTitle[0][0]}:{index_term[2]}, "
            else:
                output += f"{documentTitle[0][0]}:{index_term[2]}"
        output += f"\'"
        if len(terms) != i + 1:
            output += f","
    output += "}"
    return output
