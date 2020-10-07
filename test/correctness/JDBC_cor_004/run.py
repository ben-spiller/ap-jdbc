# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		correlator = self.apamajdbc.startCorrelator('correlator',
			config=f'{self.project.samplesDir}/default_config.yaml',
			configPropertyOverrides={"jdbc.url":"jdbc:sqlite:test.db"})
		
		correlator.injectEPL("test.mon")
		correlator.flush()
		self.waitForGrep('correlator.log', 'com.apama.adbc.ResultSetRow\(', condition='==2')
		
	def validate(self):
		self.assertGrep('correlator.log', expr=' (ERROR|FATAL) .*', contains=False)

		self.assertLineCount('correlator.log', expr='ResultSetRow\(', condition='==2')
		self.assertGrep('correlator.log', expr='ResultSetRow\(4,0,.*42\)')
		self.assertGrep('correlator.log', expr='ResultSetRow\(4,1,.*-2\)')
