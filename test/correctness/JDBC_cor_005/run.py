# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		self.skipTest("Not working yet for Small Statements 	")
		correlator = self.apamajdbc.startCorrelator('correlator',
			config=f'{self.project.samplesDir}/default_config.yaml',
			configPropertyOverrides={"jdbc.url":self.apamajdbc.getURL(),
									'jdbc.user':self.apamajdbc.getUsername(),
									'jdbc.password':self.apamajdbc.getPassword()})
		print("jdbc.url"+self.apamajdbc.getURL())
		
		correlator.injectEPL("test.mon")
		correlator.flush()
		self.waitForGrep('correlator.log', 'com.apama.adbc.StatementDone\(', condition='==5')
		
	def validate(self):
		#TODO validation not updated yet
		self.assertGrep('correlator.log', expr=' (ERROR|FATAL) .*', contains=False)
		# Query results
		self.assertOrderedGrep('correlator.log', exprList=[
			"com.apama.adbc.ResultSetRow\(5,0.*42\)",
			"com.apama.adbc.ResultSetRow\(5,1.*100\)",
			"com.apama.adbc.StatementDone\(5,-1,",])
		# Insert results
		self.assertOrderedGrep('correlator.log', exprList=[
			"com.apama.adbc.StatementDone\(1,0,",
			"com.apama.adbc.StatementDone\(2,1,",
			"com.apama.adbc.StatementDone\(3,1,",
			"com.apama.adbc.StatementDone\(4,1,",])
