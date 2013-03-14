from pyon.datastore.couchbase.standalone import CouchbaseDataStore
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.util.log import log
from pyon.core.bootstrap import CFG
from couchbase.client import Couchbase, Bucket
from pyon.public import IonObject, RT, log
from pyon.core.exception import BadRequest, Conflict, NotFound, ServerError
from time import clock, time
from pyon.ion.resource import RT, PRED, LCS
from pyon.datastore.datastore import DataStore
import time
import interface
from unittest import skip
import pprint
from interface.objects import AttachmentType

OWNER_OF = "XOWNER_OF"
HAS_A = "XHAS_A"
BASED_ON = "XBASED_ON"

database_name = "testing"

@attr('UNIT', group='datastore')
class TestCouchbaseStandalone(IonIntegrationTestCase):

    def setUp(self):
        log.debug("cfg %s", repr(CFG['server'].keys()))
        self.hostname=CFG['server']['couchbase']['host']
        self.username=CFG['server']['couchbase']['username']
        self.password=CFG['server']['couchbase']['password']
        self.resources = {}

        self.datastore_name = database_name
        self.ds = CouchbaseDataStore(datastore_name=self.datastore_name, config=CFG)
        #self._delete_and_create_datastore()

    def tearDown(self):
        #self.ds.delete_datastore()
        pass

    def _delete_and_create_datastore(self):
        try :
            self.ds.delete_datastore()
        except NotFound:
            pass
        except ServerError:
            pass

        self.ds.create_datastore()


    def test_couchbase_standalone_crud_operation (self):

        #self.ds._update_views()

        ds, ds_name = self.ds._get_datastore(self.datastore_name)
        self.assertIsInstance(ds, Bucket)

        doc = {}
        doc['test'] = "this is just a test"

        # Create
        id, rev = self.ds.create_doc(doc)

        # Read and verify create
        obj = self.ds.read_doc(id)
        self.assertTrue(type(obj)==type(doc), "Types don't match %s : %s" %(type(obj), type(doc)))
        self.assertEqual(obj['test'], doc['test'])

        # Update
        updated_data = "this has been updated"
        obj['test'] = updated_data
        id2 , rev2 = self.ds.update_doc(obj)

        # Verify update
        self.assertEqual(id, id2)
        obj2 = self.ds.read_doc(id2)
        self.assertEqual(obj2['test'], updated_data)

        # Update not-existing object
        temp = {}
        temp['test'] = "this is just a test"
        temp['_id'] = 'x2x'
        self.ds.update_doc(temp)

        # Delete
        self.ds.delete_doc(obj)
        self.ds.delete_doc(temp['_id'])

        # Verify delete
        with self.assertRaises(NotFound):
            self.ds.delete_doc(obj)
        with self.assertRaises(NotFound):
            self.ds.delete_doc(obj2)
        with self.assertRaises(NotFound):
            self.ds.read_doc(id)
            # Delete
        return

    def test_mult(self):

        doc = {}
        doc['test'] = "this is just a test"

        doc2 = {}
        doc2['test'] = "this is just second test"

        docs = [doc, doc2]
        doc3 = {}
        doc3['test'] = "this is just second test"
        # Create
        res = self.ds.create_doc_mult(docs)
        ids = [res[0][1], res[1][1]]
        read_docs = self.ds.read_doc_mult(ids)
        self.assertTrue(doc in read_docs)
        self.assertTrue(doc2 in read_docs)
        self.assertTrue(doc3 not in read_docs)


    def test_view(self):
        self.ds.define_profile_views("RESOURCES")
        self.ds._get_design_name('association')
        view = self.ds._get_view_name('association', 'by_doc')
        self.assertTrue(view.startswith('_design'))
        self.ds.refresh_views('association')
        d = self.ds.find_docs_by_view('association', 'by_doc')
        self.ds.delete_views('attachment')


