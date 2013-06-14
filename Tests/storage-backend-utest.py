##
# DBM BACKEND TEST
##

import unittest
import storage
import datetime
import random
import os

class AbstractBackendTest:

	def _createBackend( self ):
		raise NotImplementedError

	def setUp(self):
		self.path= "./" + os.path.basename(__file__).split(".")[0]
		self.backend = self._createBackend()
		self.allType = [None,0,1,-1,0.5,str(),list(),dict(),tuple(),"string",(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]

	def testAddMethod(self):
		bk = self.backend
		
		#Test keys --accepted
		keys =   ["String1","String2","String3","String4","String5","String5","String5"]
		values = ["Value1", "Value2", "Value3", "Value4", "Value5", "Value5", "Value6" ]
		
		#simple
		self.assertEqual(0,bk.count())
		bk.add(keys[0],values[0])
		self.assertEqual(1,bk.count())

		#multi
		bk.add(keys[1],values[1])
		bk.add(keys[2],values[2])
		bk.add(keys[3],values[3])
		self.assertEqual(4,bk.count())

		#same key,value addition
		bk.add(keys[4],values[4])
		self.assertEqual(5,bk.count())
		bk.add(keys[5],values[5])
		self.assertEqual(5,bk.count())
		self.assertEqual(bk.get(keys[5]),values[5])
		self.assertEqual(bk.get(keys[5]),values[4])

		#same key addition
		bk.add(keys[6],values[6])
		self.assertEqual(5,bk.count())
		self.assertEqual(bk.get(keys[6]),values[6])
		self.assertNotEqual(bk.get(keys[6]),values[5])

		#Test keys --invalid
		invalid_entry =   [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]

		#invalid key [assuming not accepted]
		for ktype in invalid_entry:
			bk.add(ktype,"Value")
			self.assertEqual(5,bk.count())

		#invalid key [assuming not accepted]
		for i,vtype in enumerate(keys):
			bk.add(repr(i),vtype)
			self.assertEqual(6+i,bk.count())

	def testUpdate(self):
		bk = self.backend

		bk.add("key1","value1")
		bk.add("key2","value2")
		bk.add("key3","value3")
		bk.add("key4","value4")

		#simple
		self.assertEqual("value1",bk.get("key1"))
		bk.update("key1","new_value")
		self.assertEqual("new_value",bk.get("key1"))

		#update undefined entry
		self.assertRaises(Exception,bk.update,"key5","value5")

		#update removed entry



class DBMBackendTest(AbstractBackendTest, unittest.TestCase):

	def _createBackend( self ):
		return storage.DBMBackend(self.path)

	def tearDown(self):
		if(os.path.exists(self.path+".db")):
			os.remove(self.path+".db")
		else:
			print "file do not exist"

if __name__ == "__main__":
	unittest.main()