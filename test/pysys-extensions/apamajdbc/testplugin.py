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
		self.JARFILE_IDX=0 #index 0
		self.CLASSNAME_IDX=1
		self.USERNAME_IDX=2
		self.PASSWORD_IDX=3
		self.URL_IDX=4
		self.DEFAULT_DB='sqlite'
		#key with jar file, classname, username, password, url
		self.VALID_JDBC_VENDOR_DATA = {'sqlite':['sqlite-jdbc-3.8.11.2.jar','org.sqlite.JDBC',None,'','jdbc:sqlite:test.db'], 
									   'mysql' :['mysql-connector-java-8.0.21.jar','com.mysql.cj.jdbc.Driver','mysql','mysql','jdbc:mysql://localhost:33060/innodb']}

	def getUsername(self):
		return self.VALID_JDBC_VENDOR_DATA[self.DEFAULT_DB][self.USERNAME_IDX]
	def getDriver(self):
		return self.VALID_JDBC_VENDOR_DATA[self.DEFAULT_DB][self.JARFILE_IDX]
	def getPassword(self):
		return self.VALID_JDBC_VENDOR_DATA[self.DEFAULT_DB][self.PASSWORD_IDX]
	def getURL(self):
		return self.VALID_JDBC_VENDOR_DATA[self.DEFAULT_DB][self.URL_IDX]

	def startCorrelator(self, name, **kwargs):
		"""
		A wrapper for setting up a CorrelatorHelper suitable of JDBC testing; sets Java, the classpath, injects necessary EPL.
		TODO: maybe remove this in Apama 10.7 when the standard apama test plugin has the same functionality. 
		"""
		c = apama.correlator.CorrelatorHelper(self.owner, name=name)
		c.addToClassPath('{testRootDir}/../lib/{driver}'.format(testRootDir=self.project.testRootDir,driver=self.getDriver()))
		
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
