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

	def startCorrelator(self, name, **kwargs):
		"""
		A wrapper for setting up a CorrelatorHelper suitable of JDBC testing; sets Java, the classpath, injects necessary EPL.
		TODO: maybe remove this in Apama 10.7 when the standard apama test plugin has the same functionality. 
		"""
		c = apama.correlator.CorrelatorHelper(self.owner, name=name)
		c.addToClassPath(f'{self.project.testRootDir}/../lib/sqlite-jdbc-3.8.11.2.jar')
		kwargs.setdefault("configPropertyOverrides", {})
		kwargs["configPropertyOverrides"]["jdbc.connectivityPluginDir"] = self.project.appHome
		c.start(logfile=name+'.log', java=True, **kwargs)
		if(kwargs.get("waitForServerUp", True)):
			c.injectEPL([
				self.project.APAMA_HOME + '/monitors/ConnectivityPluginsControl.mon',
				self.project.APAMA_HOME + '/monitors/ConnectivityPlugins.mon',
				self.project.APAMA_HOME + '/monitors/AutomaticOnApplicationInitialized.mon',
				self.project.eventDefDir+'/ADBCEvents.mon',])
			c.sendEventStrings("com.apama.connectivity.ApplicationInitialized()")
		return c
	
class ApamaJDBCBaseTest(apama.basetest.ApamaBaseTest):
	""" Tiny stub class to enable using the ApamaJDBCPlugin with self.jdbc. 
	TODO: Remove this when we have Apama 10.7 as the latest PySys has built-in plugin support. 
	"""
	def setup(self, **kwargs):
		super(ApamaJDBCBaseTest, self).setup(**kwargs)
		self.apamajdbc = ApamaJDBCPlugin()
		self.apamajdbc.setup(self)
