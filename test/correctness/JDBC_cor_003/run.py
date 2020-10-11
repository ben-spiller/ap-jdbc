# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		correlator = self.apamajdbc.startCorrelator('correlator',
			config=f'{self.project.samplesDir}/default_config.yaml',
			configPropertyOverrides={"jdbc.url":self.apamajdbc.getURL(),
									'jdbc.user':self.apamajdbc.getUsername(),
									'jdbc.password':self.apamajdbc.getPassword()})
		print("jdbc.url"+self.apamajdbc.getURL())
		
		correlator.injectEPL("test.mon")
		correlator.flush()
		self.waitForGrep('correlator.log', 'com.apama.adbc.SQLStatementDone\(', condition='==5',
			process=correlator.process, errorExpr=[' (ERROR|FATAL|WARN) .*'])

	def validate(self):
		self.assertGrep('correlator.log', expr=' (ERROR|FATAL) .*', contains=False)
		# TODO: hardcoding the event strings is brittle; rewrite this using the Apama 10.7 JSON event validation mechanism
		# Query results
		self.assertOrderedGrep('correlator.log', exprList=[
			"com.apama.adbc.ResultSetRow\(5,0.*42\)",
			"com.apama.adbc.ResultSetRow\(5,1.*100\)",
			"com.apama.adbc.SQLStatementDone\(5,1,false,optional\(\)\)"]) # query succeeds and returns 1 row 
		# Insert results
		self.assertOrderedGrep('correlator.log', exprList=[
			"com.apama.adbc.SQLStatementDone\(1,0,", #1 modifies no rows
			"com.apama.adbc.SQLStatementDone\(2,1,", #2 modifies 1 row
			"com.apama.adbc.SQLStatementDone\(3,1,",
			"com.apama.adbc.SQLStatementDone\(4,1,",])
