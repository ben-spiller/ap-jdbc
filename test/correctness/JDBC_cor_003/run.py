# Copyright (c) 2020 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors. 
# Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG 

import pysys
import apamajdbc.testplugin
from pysys.constants import *

class PySysTest(apamajdbc.testplugin.ApamaJDBCBaseTest):

	def execute(self):
		correlator = self.apamajdbc.startCorrelator('correlator',
			config=f'{self.project.samplesDir}/default_config.yaml',
			configPropertyOverrides={"jdbc.connectivityPluginDir":self.project.appHome, "jdbc.url":"jdbc:sqlite:test.db"})
		correlator.injectEPL([self.project.eventDefDir+'/ADBCEvents.mon'])
		
		correlator.flush()
		#self.waitForGrep('correlator.log', expr="Loaded simple test monitor", 
		#	process=correlator.process, errorExpr=[' (ERROR|FATAL) .*'])
		
	def validate(self):
		# look for log statements in the correlator log file
		self.assertGrep('correlator.log', expr=' (WARN|ERROR|FATAL) .*', contains=False)
