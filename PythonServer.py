import glob
import sys
import socket
import sys

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

import os.path

from chord import FileStore
from chord.ttypes import NodeID, RFile, RFileMetadata, SystemException

from hashlib import sha256

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def node_instance(ip, port, id):
	n = NodeID()
	n.ip = ip
	n.port = int(port)
	n.id = id
	return n

rfile_list = []

def getCurrNode():
	ip = socket.gethostbyname(socket.getfqdn())
	port = sys.argv[1]
	id = sha256(str(ip) + ":" + str(port)).hexdigest()
	return node_instance(ip, port, id)
	
def belongsTo(current_node, preceding_node, successor_node):
	if preceding_node < successor_node:
		return preceding_node <= current_node < successor_node
	return preceding_node <= current_node or current_node < successor_node

class FileStoreHandler:
	node = getCurrNode()
	
	def __init__(self):
		self.node_list = []
		self.curr_node = self.node
	
	def writeFile(self, rFile):
		self.curr_node = getCurrNode()
		node = self.findSucc(rFile.meta.contentHash)
		if node.port == self.node.port and node.ip == self.node.ip:
                    if not rFile in rfile_list:
                        temp = RFile()
                        tempMeta = RFileMetadata()
                        tempMeta.filename = rFile.meta.filename
                        tempMeta.version = 0
                        tempMeta.contentHash = rFile.meta.contentHash
                        temp.content = rFile.content
                        temp.meta = tempMeta
                        rfile_list.append(temp)
                    else:
                        for i in rfile_list:
                            if i.meta.filename == rFile.meta.filename:
                                ver = i.meta.version
                                rfile_list.remove(i)
                                break
                        temp = RFile()
                        tempMeta = RFileMetadata()
                        tempMeta.filename = rFile.meta.filename
                        tempMeta.version = ver + 1
                        tempMeta.contentHash = rFile.meta.contentHash
                        temp.content = rFile.content
                        temp.meta = tempMeta
                        rfile_list.append(temp)
                else:
                        s = SystemException()
			s.message = "Key not associated"
			raise s


	def readFile(self, filename):
		self.curr_node = getCurrNode()
		node = self.findSucc(sha256(filename).hexdigest())
		
		if node.port == self.node.port and node.ip == self.node.ip:
                    for i in rfile_list:
                        if i.meta.filename == filename:
                                return i
		else:
		    s = SystemException()
	            s.message = "No file exist with the name " + filename
		    raise s

	def setFingertable(self,node_list):
		try:
			self.node_list = node_list
		except:
			s = SystemException()
			s.message = "Cannot set finger tables"
			raise s
			
	def findSucc(self, key):
		if not self.node_list:
			s = SystemException()
			s.message = "Finger Table does not exist"
			raise s
		self.curr_node = getCurrNode()
		self.curr_node = self.findPred(key)
                if self.node.ip == self.curr_node.ip and self.node.port == self.curr_node.port:
                    return self.getNodeSucc()
                transport = TSocket.TSocket(self.curr_node.ip, self.curr_node.port)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                tNode = FileStore.Client(protocol)
                transport.open()
		targetNode = tNode.getNodeSucc()
                transport.close()
		return targetNode
	
	def findPred(self,key):
		self.curr_node = getCurrNode()
		if self.getNodeSucc().id == self.curr_node.id:
			return self.curr_node
		if not belongsTo(key, self.curr_node.id, self.getNodeSucc().id):
			curr_node = self.closeFinger(key)
                        transport = TSocket.TSocket(curr_node.ip, curr_node.port)
                        transport = TTransport.TBufferedTransport(transport)
                        protocol = TBinaryProtocol.TBinaryProtocol(transport)
			sockObject = FileStore.Client(protocol)
                        transport.open()
                        predSock = sockObject.findPred(key)
                        transport.close()
			return predSock
				
	def closeFinger(self,key):
		for x in range(255, -1, -1):
			if belongsTo(self.node_list[x].id, self.curr_node.id, key) and self.node_list[x].id != self.curr_node.id:
				return self.node_list[x]
		return self.curr_node
			
	def getNodeSucc(self):
		if not self.node_list:
			s = SystemException()
			s.message = "Finger table does not exist"
			raise s
		node = self.node_list[0]
		return node


if __name__ == '__main__':
	handler = FileStoreHandler()
	processor = FileStore.Processor(handler)
	transport = TSocket.TServerSocket(port=int(sys.argv[1]))
	hostname = socket.gethostname()
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()

	server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
	# You could do one of these for a multithreaded server
	# server = TServer.TThreadedServer(
	#	processor, transport, tfactory, pfactory)
	print('Starting the server...')
	server.serve()
