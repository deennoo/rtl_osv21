# -*- coding: utf-8 -*-

"""
Function for parsing data packets from Oregon Scientific weather sensors
"""

from utils import computeDewPoint, computeWindchill, computeSeaLevelPressure

__version__ = '0.1'
__all__ = ['nibbles2value', 'computeChecksum', 'parsePacketv21', 'parseBitStream', 
           '__version__', '__all__']


def nibbles2value(nibbles):
	"""
	Convert a sequence of bits into list of integer nibbles.
	"""
	
	# A nibbles is 4 bits
	n = len(nibbles)/4
	
	# Loop over the nibbles
	out = []
	for i in xrange(n):
		out.append( (nibbles[4*i+3]<<3) | (nibbles[4*i+2]<<2) | (nibbles[4*i+1]<<1) | nibbles[4*i+0] )
		
	# Done
	return out


def computeChecksum(bits):
	"""
	Compute the byte-based checksum for a sequence of bits.
	"""
	
	# Bits -> Integers
	values = nibbles2value(bits)
	
	# Sum
	value = sum(values)
	
	# Convert to an 8-bit value
	value = (value & 0xFF) + (value >> 8)
	
	# Done
	return value


def _parseBTHGN129(data):
	"""
	Parse the data section of a BTHGN129 outdoor temperature/humidity/pressure
	sensor packet and return a dictionary of the values recovered.
	"""
	
	output = {'temperature': -99, 'humidity': -99, 'pressure': -99, 
			  'comfortLevel': 'unknown', 'forecast': 'unknown'}
			  
	# Indoor temperature in C
	temp = nibbles2value(data[0:12])
	temp = 10*temp[2] + temp[1] + 0.1*temp[0]
	if sum(data[12:16]) > 0:
		temp *= -1
	output['temperature'] = temp
	
	# Indoor relative humidity as a percentage
	humi = nibbles2value(data[16:24])
	output['humidity'] = 10*humi[1]+humi[0]
		
	# Indoor "comfort level"
	comf = nibbles2value(data[24:28])[0]
	if comf == 0:
		output['comfortLevel'] = 'normal'
	elif comf == 4:
		output['comfortLevel'] = 'comfortable'
	elif comf == 8:
		output['comfortLevel'] = 'dry'
	elif comf == 0xC:
		output['comfortLevel'] = 'wet'
	else:
		output['comfortLevel'] = 'unknown'
		
	# Barometric pressure in mbar
	baro = nibbles2value(data[28:36])
	baro = (baro[1] << 4) | (baro[0])
	if baro >= 128:
		baro -= 256
	#output['pressure'] = baro + 856
	output['pressure'] = baro + 1071
		
	# Pressure-based weather forecast
	fore = nibbles2value(data[40:44])[0]
	if fore == 2:
		output['forecast'] = 'cloudy'
	elif fore == 3:
		output['forecast']  = 'rainy'
	elif fore == 6:
		output['forecast']  = 'partly cloudy'
	elif fore == 0xC:
		output['forecast']  = 'sunny'
	else:
		output['forecast']  = 'unknown'
		
	return output

def _parsePCR800(data):
	"""
	Parse the data section of a PCR800 rain gauge packet and return a dictionary 
	of the values recovered.
	"""
	
	output = {'rainrate': -99, 'rainfall': -99}
	
	# Rainfall rate in mm/hr
	rrate = nibbles2value(data[0:12])
	output['rainrate'] = 10*rrate[2] + rrate[1] + 0.1*rrate[0]
	
	# Total rainfall in mm
	rtotl = nibbles2value(data[12:32])
	output['rainfall'] = 1000*rtotl[4] + 100*rtotl[3] + 10*rtotl[2] + rtotl[1] + 0.1*rtotl[0]
	
	return output

def _parseWGR800(data):
	"""
	Parse the data section of a WGR968 anemometer packet and return a dictionary 
	of the values recovered.
	"""
	
	output = {'average': -99, 'gust': -99, 'direction': -99}
	
	# Wind direction in degrees (N = 0)
	wdir = nibbles2value(data[0:12])
	output['direction'] = 100*wdir[2] + 10*wdir[1] + wdir[0]
	
	# Gust wind speed in m/s
	gspd = nibbles2value(data[12:24])
	output['gust'] = 10*gspd[2] + gspd[1] + 0.1*gspd[0]
	
	# Average wind speed in m/s
	aspd = nibbles2value(data[24:36])
	output['average'] = 10*aspd[2] + aspd[1] + 0.1*aspd[0]
	
	return output
	
def _parseTHGN800(data):
	"""
	Parse the data section of a THGN800 temperature/humidity sensor packet and return a dictionary 
	of the values recovered.
	"""
	
	output = {'temperature': -99, 'humidity': -99}
	
	# Temperature in C
	temp = nibbles2value(data[0:12])
	temp = 10*temp[2] + temp[1] + 0.1*temp[0]
	if sum(data[64:68]) > 0:
		temp *= -1
	output['temperature'] = temp
		
	# Relative humidity as a percentage
	humi = nibbles2value(data[16:24])
	output['humidity'] = 10*humi[1]+humi[0]
	
	return output
	
def _parseUVR128(data):
	"""
	Parse the data section of a RVY128 UV sensor packet and return a dictionary of the
	values recovered.
	"""
	
	output = {'uvIndex': -99}
	
	# UV index
	uv = nibbles2value(data[0:8])
	uv = 10*uv[1] + uv[0]
	output['uvIndex'] = uv
	
	return output
	
def parsePacketv21(packet, wxData=None, verbose=False):
	"""
	Given a sequence of bits try to find a valid Oregon Scientific v2.1 
	packet.  This function returns a status code of whether or not the packet
	is valid, the sensor name, the channel number, and a dictionary of the 
	values recovered.
	
	Supported Sensors:
	  * 5D53 - BTHGN129 - Outdoor temperature/humidity/pressure
	  * 1924 - PCR800  - Rain gauge
	  * 1984 - WGR800  - Anemometer
	  * FA28 - THGN800 - Outdoor temperature/humidity
	  
	  * EC70 - UVR128  - UV sensor
	"""
	
	# Check for a valid preamble
	if sum(packet[:16]) != 16:
		return False, 'Invalid', -1, {}
	
	# Check for a valid sync word.
	if nibbles2value(packet[16:20])[0] != 10:
		return False, 'Invalid', -1, {}
		
	# Try to figure out which sensor is present so that we can get 
	# the packet length
	sensor = ''.join(["%x" % i for i in nibbles2value(packet[20:36])])
	if sensor == '5d53':
		nm = 'BTHGN129'
		ds = 96
	elif sensor == '2A19':
		nm = 'PCR800'
		ds = 84
	elif sensor == '1A89':
		nm = 'WGR800'
		ds = 88
	elif sensor == 'FA28':
		nm = 'THGN800'
		ds = 80
	elif sensor == 'ec70':
		nm = 'UVR128'
		ds = 68
	else:
		## Unknown - fail
		return False, 'Invalid', -1, {}
			
	# Make sure there are enough bits that we get a checksum
	if len(packet) < ds+8:
		return False, 'Invalid', -1, {}
		
	# Report
	if verbose:
		print 'preamble ', packet[ 0:16], ["%x" % i for i in nibbles2value(packet[0:16])]
		print 'sync     ', packet[16:20], ["%x" % i for i in nibbles2value(packet[16:20])]
		print 'sensor   ', packet[20:36], ["%x" % i for i in nibbles2value(packet[20:36])]
		print 'channel  ', packet[36:40], ["%x" % i for i in nibbles2value(packet[36:40])]
		print 'code     ', packet[40:48], ["%x" % i for i in nibbles2value(packet[40:48])]
		print 'flags    ', packet[48:52], ["%x" % i for i in nibbles2value(packet[48:52])]
		print 'data     ', packet[52:ds], ["%x" % i for i in nibbles2value(packet[52:ds])]
		print 'checksum ', packet[ds:ds+8], ["%x" % i for i in nibbles2value(packet[ds:ds+8])]
		print 'postamble', packet[ds+8:ds+16]
		print '---------'
		
	# Compute the checksum and compare it to what is in the packet
	ccs = computeChecksum(packet[20:ds])
	ccs1 = ccs & 0xF
	ccs2 = (ccs >> 4) & 0xF
	ocs1, ocs2 = nibbles2value(packet[ds:ds+8])
	if ocs1 != ccs1 or ocs2 != ccs2:
		return False, 'Invalid', -1, {} 
	
	# Parse
	data = packet[52:ds]
	channel = nibbles2value(packet[36:40])[0]
	if nm == 'BTHGN129':
		output = _parseBTHGN129(data)
	elif nm == 'PCR800':
		output = _parsePCR800(data)
	elif nm == 'WGR800':
		output = _parseWGR800(data)
	elif nm == 'THGN800':
		output = _parseTHGN800(data)
	elif nm == 'UVR128':
		output = _parseUVR128(data)
	else:
		return False, 'Invalid', -1, {}
		
	# Report
	if verbose:
		print output
		
	# Return the packet validity, channel, and data dictionary
	return True, nm, channel, output


def parseBitStream(bits, elevation=0.0, inputDataDict=None, verbose=False):
	"""
	Given a sequence of bits from readRTL/readRTLFile, find all of the 
	valid Oregon Scientific v2.1 packets and return the data contained
	within the packets as a dictionary.  In the process, compute various
	derived quantities (dew point, windchill, and sea level corrected
	pressure).
	
	.. note::
		The sea level corrected pressure is only compute if the elevation 
		(in meters) is set to a non-zero value.  
	"""
	
	# Setup the output dictionary
	output = {}
	if inputDataDict is not None:
		for key,value in inputDataDict.iteritems():
			output[key] = value
			
	# Find the packets and save the output
	i = 0
	while i < len(bits)-32:
		## Check for a valid preamble (and its logical negation counterpart)
		if sum(bits[i:i+32:2]) == 16 and sum(bits[i+1:i+1+32:2]) == 0:
			### Assume nothing
			valid = False
			
			### Packet #1
			packet = bits[i+0::2]
			try:
				valid, sensorName, channel, sensorData = parsePacketv21(packet, verbose=verbose)
			except IndexError:
				pass
				
			if not valid:
				### Packet #2
				packet = bits[i+1::2]
				try:
					valid, sensorName, channel, sensorData = parsePacketv21(packet, verbose=verbose)
				except IndexError:
					pass
				
			### Data reorganization and computed quantities
			if valid:
				#### Dew point - indoor and output
				if sensorName in ('BTHGN129', 'THGN800', 'THGN800'):
					sensorData['dewpoint'] = computeDewPoint(sensorData['temperature'], sensorData['humidity'])
				
				#### Disentangle the indoor temperatures from the outdoor temperatures
				if sensorName == 'BTHGN129':
					for key in ('temperature', 'humidity', 'dewpoint','pressure'):
						output[key] = sensorData[key]
				#### Multiplex the THGN800 values
				for key in sensorData.keys():
					if key in ('temperature', 'humidity', 'dewpoint'):
						if sensorName == 'THGN800':
							output[key] = sensorData[key]
						else:
							try:
								output['alt%s' % key.capitalize()][channel-1] = sensorData[key]
							except KeyError:
								output['alt%s' % key.capitalize()] = [None, None, None, None]
								output['alt%s' % key.capitalize()][channel-1] = sensorData[key]
					else:
						output[key] = sensorData[key]
						
		i += 1
		
	# Compute combined quantities
	if 'temperature' in output.keys() and 'average' in output.keys():
		output['windchill'] = computeWindchill(output['temperature'], output['average'])
		
	# Done
	return output
