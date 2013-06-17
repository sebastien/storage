##
# DBM BACKEND TEST
##

import sys
sys.path.append("../")

import Sources.storage.__init__ as storage

import unittest
import datetime
import random
import os
import shutil

class AbstractBackendTest:

	def _createBackend( self ):
		raise NotImplementedError

	def setUp(self):
		self.path= "./" + os.path.basename(__file__).split(".")[0]
		self.backend = self._createBackend()
		self.backend.clear()
		
	def testAdd(self):
		bk = self.backend
		
		#Test keys --accepted
		keys =   ["String1","String2","String3","String4","String5","String5","String5"]
		values = ["Value1", "Value2", "Value3", "Value4", "Value5", "Value5", "Value6" ]
		
		a = bk.keys()
		for i in a:
			print (i)
		
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
		self.assertMultiLineEqual(bk.get(keys[6]),values[6])
		self.assertNotEqual(bk.get(keys[6]),values[5])

		#Test keys --invalid
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]

		#invalid key [assuming not accepted]
		for ktype in invalid_entry:
			bk.add(ktype,"Value")
			self.assertEqual(5,bk.count())

		#invalid value [assuming accepted]
		for i,vtype in enumerate(keys):
			bk.add(repr(i),vtype)
			self.assertEqual(6+i,bk.count())
			
		#stress test TODO

	def testUpdate(self):
		bk = self.backend

		bk.add("key1","value1")
		bk.add("key2","value2")
		bk.add("key3","value3")
		bk.add("key4","value4")
		count = 4

		#simple
		self.assertMultiLineEqual("value1",bk.get("key1"))
		bk.update("key1","new_value")
		self.assertMultiLineEqual("new_value",bk.get("key1"))

		#update undefined entry
		bk.update("key5","value5")
		count+=1

		#update removed entry
		bk.remove("key1")
		bk.update("key1","value4")
		self.assertEqual(count,bk.count())
		self.assertMultiLineEqual(bk.get("key1"),"value4")
		
		#invalid key [IndexError]
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]
		
		for tkey in invalid_entry:
			self.assertRaises(Exception,bk.update,tkey,"value")
		
	def testRemove(self):
		bk = self.backend
		
		bk.add("key1","value1")
		bk.add("key2","value2")
		bk.add("key3","value3")
		bk.add("key4","value4")
		
		#simple
		self.assertEqual(bk.count(),4)
		bk.remove("key1")
		self.assertEqual(bk.count(),3)
		self.assertIsNone(bk.get("key1"))
		
		#remove undefined entry [IndexError]
		self.assertRaises(Exception,bk.remove,"key1")
		
		#invalid keys [IndexError]
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]
		for tkey in invalid_entry:
			self.assertRaises(Exception,bk.remove,tkey)
			
		#stress test TODO
		
	def testSync(self):
		pass
		
	def testHas(self):
		bk = self.backend
		
		#undefined
		self.assertFalse(bk.has("key"))
		
		#existing
		bk.add("key","value")
		self.assertTrue(bk.has("key"))
		self.assertTrue(bk.has("key"))
		self.assertTrue(bk.has("key"))
		
		#removed
		bk.remove("key")
		self.assertFalse(bk.has("key"))
		
	def testGet(self):
		bk = self.backend
		
		#undefined
		self.assertIsNone(bk.get("key"))
		
		#existing
		bk.add("key","value")
		self.assertMultiLineEqual("value",bk.get("key"))
		self.assertMultiLineEqual("value",bk.get("key"))
		
		#removed
		bk.remove("key")
		self.assertIsNone(bk.get("key"))
		
	def testKeys(self):
		bk = self.backend
		
		#empty database
		self.assertListEqual(bk.keys(),[])
		
		#keys
		keys = ["key1","key2","key3","key4"]
		for k in keys:
			bk.add(k,"value")
		self.assertListEqual(keys,bk.keys())
		
		#removed
		bk.remove("key3")
		bk.remove("key4")
		self.assertListEqual(bk.keys(),keys[:2])
		
	def testClear(self):
		bk = self.backend
		
		#clear empty database
		self.assertEqual(0,bk.count())
		bk.clear()
		self.assertEqual(0,bk.count())
		
		#clear database
		keys = ["key1","key2","key3","key4"]
		for k in keys:
			bk.add(k,"value")
			
		self.assertNotEqual(0,bk.count())
		bk.clear()
		self.assertEqual(0,bk.count())
		
	def testList(self):
		bk = self.backend
		
		#empty database
		l = []
		for item in bk.list():
			l += [item]
		self.assertListEqual([],l)
		
		#list
		keys   = ["key1","key2","key3","key4","key5"]
		values = ["val1","val2","val3","val4","val4"]
		for i in range(len(keys)):
			bk.add(keys[i],values[i])
			
		l=[]
		for item in bk.list():
			l += [item]
		self.assetListEqual(l,values)
		
	def testCount(self):
		bk = self.backend
		
		#empty
		self.assertEqual(0,bk.count())
		
		#sample
		keys = ["key1","key2","key3","key4"]
		for k in keys:
			bk.add(k,"value")
		self.assertEqual(len(keys),bk.count())
		self.assertEqual(len(keys),bk.count())
		
		#remove
		bk.remove(keys[-1])
		bk.remove(keys[-2])
		self.assertEqual(len(keys)-2,bk.count())
		


class DBMBackendTest(AbstractBackendTest, unittest.TestCase):

	def _createBackend( self ):
		#erase the file to clear the database
		if (os.path.exists(self.path+".db")):
			os.remove(self.path+".db")
		return storage.DBMBackend(self.path)
		
	def tearDown(self):
		self.backend.close()
		if (os.path.exists(self.path+".db")):
			os.remove(self.path+".db")

	def testClose(self):
		bk = self.backend
		
		#close
		bk.add("key","value")
		bk.close()
		self.assertRaises(Exception,bk.update,"key","new_value")
		
		#closed backend
		self.assertRaises(Exception,bk.close)
		
class MemoryBackendTest(AbstractBackendTest, unittest.TestCase):
	
	def _createBackend(self):
		return storage.MemoryBackend()

class DirectoryBackendTest(AbstractBackendTest, unittest.TestCase):

	def _createBackend(self):
		return storage.DirectoryBackend(os.getcwd()+"/test-dir")
		
	@classmethod
	def setUpClass(cls):
		if not (os.path.exists(os.getcwd()+"/test-dir")):
			os.mkdir(os.getcwd()+"/test-dir")
		
	@classmethod
	def tearDownClass(cls):
		if (os.path.exists(os.getcwd()+"/test-dir")):
			shutil.rmtree(os.getcwd()+"/test-dir")
		
	def setUp(self):
		self.backend = self._createBackend()
		keys = self.backend.keys()
		for k in keys:
			self.backend.remove(k)
		
	def testGetFileName(self):
		bk = self.backend
		
		#undefined item
		self.assertIsNone(bk.getFileName("file"))
		
		#get file
		bk.add("file","data")
		self.assertIsNotNone(bk.getFileName("file"))
		self.assertIsNot("",bk.getFileName("file"))
		print bk.getFileName("file")
		
		#invalid filename
		bk.add("my./.file","data")
		
		
	
if __name__ == "__main__":
	unittest.main()
