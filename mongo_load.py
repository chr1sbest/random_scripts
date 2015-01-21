"""
This script moves data from one Mongo collection to another. Additionally,
the records can be altered by passing in an optional 'transform' function.
"""
from unittest import TestCase
from pymongo import MongoClient
from mongomock import Connection as MongoMock

def move_records(from_db, from_coll, to_db, to_coll, transform=None, 
                 query=None, client=MongoClient('localhost')):
    """
    Iterate through all records in a collection and copy them over to a new
    collection. Records failures in failures.txt within the directory that the
    script is executed from.

    @from_db, from_coll, to_db, to_coll: Strings that correspond to the 
        to/from databases and collections.
    @transform: Function that can alter the records before inserting them into
        the new collection. By default there will be no transformation.
    @client: MongoClient object that corresponds to the Mongo instance we are
        communicating with. Connects to local mongo instance by default.
    @query: Mongo query object if we want to iterate over a particular subset
        of data instead of the entire collection.
    """
    from_collection = client[from_db][from_coll]
    to_collection = client[to_db][to_coll]
    total_records = from_collection.count()
    # We will iterate over all records unless a particular query is specified
    query = query or {}
    # We will use the transform function passed in or by default do nothing
    transform = transform or lambda x: x
    # Iterate over the records in from_collection.
    for index, record in enumerate(from_collection.find(query)):
        # Transform them using the transform function or default (lambda x: x)
        transformed_record = transform(record)
        # Insert the transformed record into the to_collection
        try:
            result = to_collection.insert(transformed_record)
        except:
        # If the result is a failure, we want to append the record to a file.
            with open('failures.txt', 'a') as failures:
                failures.write(record)
        print '{} out of {} records copied\r'.format(index + 1, total_records),
    finished_string = '\nCopying data from {}.{} to {}.{} completed.'
    print finished_string.format(from_db, from_coll, to_db, to_coll)

class TestMoveRecords(TestCase):
    def setUp(self):
        """ Set up MongoMock collections and databases """
        self.client = MongoMock()
        self.from_collection = self.client.from_db.from_coll
        self.to_collection = self.client.to_db.to_coll
        self.from_collection.insert({'name':'testing1', 'value': 5})
        self.from_collection.insert({'name':'testing2', 'value': 10})
    
    def testMoveRecords(self):
        """
        Tests that records are moved from one collection to another
        """
        move_records('from_db', 'from_coll', 'to_db', 'to_coll',
                     client=self.client)
        testing1 = self.to_collection.find_one({'name': 'testing1'})
        testing2 = self.to_collection.find_one({'name': 'testing2'})
        self.assertEquals(testing1['value'], 5)
        self.assertEquals(testing2['value'], 10)

    def testTransform(self):
        """
        Test that we are able to define and pass in a transform function
        that will alter the resulting data in the to_collection.
        """
        # double function will double the 'value'
        def double(record):
            record['value'] = record['value'] * 2
            return record
        # pass the double function in as the transform
        move_records('from_db', 'from_coll', 'to_db', 'to_coll',
                     client=self.client, transform=double)
        testing1 = self.to_collection.find_one({'name': 'testing1'})
        testing2 = self.to_collection.find_one({'name': 'testing2'})
        self.assertEquals(testing1['value'], 10)
        self.assertEquals(testing2['value'], 20)
