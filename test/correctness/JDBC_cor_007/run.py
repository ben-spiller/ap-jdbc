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
			config=f'{self.project.samplesDir}/default_config.yaml',
            configPropertyOverrides=configPropertyOverrides
			)
		
		correlator.injectEPL([self.project.APAMA_HOME+'/monitors/JSONPlugin.mon', "test.mon"])
		correlator.flush()
		self.waitForGrep('correlator.log', 'Got SQLStatementDone with errorDetails', 
			process=correlator.process, errorExpr=[' (ERROR|FATAL|WARN) .*'], ignores=['Failed to execute statement'])
		
		
	def validate(self):
		details = json.loads(self.getExprFromFile('correlator.log', 'Got SQLStatementDone with errorDetails: (.*)'))
		
		self.assertThat('value == expected', value__eval="details['isTransientOrRecoverable']", expected=False)
		self.log.info('TODO: validate all the other error details') #  (but in a DB-driver agnostic way, of course)
	