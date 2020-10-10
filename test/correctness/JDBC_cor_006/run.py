# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		configPropertyOverrides={"jdbc.url":self.apamajdbc.getURL(),
									'jdbc.user':self.apamajdbc.getUsername(),
									'jdbc.password':self.apamajdbc.getPassword()}
		
		correlator = self.apamajdbc.startCorrelator('correlator', #verbosity='debug',
			config=f'{self.project.samplesDir}/default_config.yaml',
            configPropertyOverrides=configPropertyOverrides
			)
		
		correlator.injectEPL("test.mon")
		correlator.flush()
		self.waitForGrep('correlator.log', 'SQLStatementDone\(', condition='==5',
			process=correlator.process, errorExpr=[' (ERROR|FATAL|WARN) .*'])
		
		
	def validate(self):
		self.assertGrep('correlator.log', expr=' (ERROR|FATAL|WARN) .*', contains=False)
		
		self.logFileContents('correlator.log', includes=['ResultSetRow\(.*'])

		# TODO: add more detailed checks of what was committed 
		self.assertGrep('correlator.log', expr='ResultSetRow\(.*,200')
		self.assertGrep('correlator.log', expr='ResultSetRow\(.*,1000')
		self.assertGrep('correlator.log', expr='ResultSetRow\(.*,3000')
