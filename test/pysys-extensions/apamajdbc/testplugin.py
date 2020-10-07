import sys
import os
import logging

import pysys
import apama.correlator
import apama.basetest

class ApamaJDBCPlugin(object):
	"""
	This is a test plugin providing methods to help with Apama-JDBC testing. 
	"""

	def setup(self, testObj):
		self.owner = testObj
		self.project = self.owner.project
		self.log = logging.getLogger('pysys.ApamaJDBCPlugin')

	def getProperties(self):
		"""
		Get the -D properties dict that should be passed to the correlator to make use of this plugin. 
		"""
		return {
			'jdbc.connectivityPluginDir': self.project.appHome,
			'jdbc.url': 'localhost:000/invalidURL',
			}
		
	def startCorrelator(self, name, **kwargs):
		"""
		A wrapper around calling the CorrelatorHelper constructor and start method that sets Java and the correct classpath for JDBC
		testing.
		TODO: maybe remove this in Apama 10.7 when the standard apama test plugin has the same functionality. 
		"""
		c = apama.correlator.CorrelatorHelper(self.owner, name=name)
		c.addToClassPath(f'{self.project.testRootDir}/../lib/sqlite-jdbc-3.8.11.2.jar')
		c.start(logfile=name+'.log', java=True, **kwargs)
		return c
	
class ApamaJDBCBaseTest(apama.basetest.ApamaBaseTest):
	""" Tiny stub class to enable using the ApamaJDBCPlugin with self.jdbc. 
	TODO: Remove this when we have Apama 10.7 as the latest PySys has built-in plugin support. 
	"""
	def setup(self, **kwargs):
		super(ApamaJDBCBaseTest, self).setup(**kwargs)
		self.apamajdbc = ApamaJDBCPlugin()
		self.apamajdbc.setup(self)
