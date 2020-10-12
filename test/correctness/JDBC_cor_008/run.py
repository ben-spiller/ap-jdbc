# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 
import json

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		configPropertyOverrides={"jdbc.url":self.apamajdbc.getURL(),
									'jdbc.user':self.apamajdbc.getUsername(),
									'jdbc.password':self.apamajdbc.getPassword()}
		
		correlator = self.apamajdbc.startCorrelator('correlator', #verbosity='debug',
			config=f'{self.input}/config.yaml',
            configPropertyOverrides=configPropertyOverrides
			)
		
		correlator.injectEPL(["test.mon"])
		correlator.flush()
		self.waitForGrep('correlator.log', 'SQLStatement is done', condition="==3", 
			process=correlator.process, errorExpr=[' (ERROR|FATAL|WARN) .*'])
		self.waitForGrep('correlator.log', 'Got CommitDone', condition=">=1", 
			process=correlator.process, errorExpr=[' (ERROR|FATAL|WARN) .*'])
		
		
	def validate(self):
		# TODO: add some queries then we can verify this
		pass