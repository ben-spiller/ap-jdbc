# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		correlator = self.apamajdbc.startCorrelator('correlator', config=self.project.samplesDir+'/default_config.yaml', 
			configPropertyOverrides={"jdbc.url":"localhost:000/invalidURL",
									'jdbc.user':self.apamajdbc.getUsername(),
									'jdbc.password':self.apamajdbc.getPassword()})
		
		self.waitForGrep('correlator.log', expr="Failed to connect to JDBC", 
			process=correlator.process, errorExpr=[' (FATAL) .*'])
		
	def validate(self):
		# look for log statements in the correlator log file
		#self.assertGrep('correlator.log', expr=' (ERROR|FATAL) .*', contains=False)
		self.assertGrep('correlator.log', expr=' ERROR .*Failed to connect to JDBC: .*No suitable driver')
		self.assertGrep('correlator.log', expr='NullPointerException', contains=False)
