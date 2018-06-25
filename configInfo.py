import sys
import os
import string
import shutil

if os.name == 'posix':
    # Change below to where HTi looks for it
    configDATFileLocation = '/etc/hobart/config_linux.dat'
    configCDATFileLocation = '/etc/hobart/configc.dat'
elif os.name == 'nt':
    # Change below to where HTx looks for it (soon it will be C:\Program Files\Hobart\config.dat and configc.dat)
    configDATFileLocation = 'C:\Program Files\Hobart\config.dat'
    configCDATFileLocation = 'C:\Program Files\Hobart\configc.dat'
    
def getConfigFileLocation():
    return configDATFileLocation
    
def getPythonInstallPath():
    if os.name == 'nt':
        return 'C:\\Python27'
    else:
        # TODO
        return ''

def getRootFolder():
    if os.name == 'nt':
        answer = "C:\\Progam Files\\Hobart"
        return answer
        #return getValue('systemUserRootDir').replace('\\\\',  '\\')
    else:
        return getValue('systemUserRootDir')

def file_exists(filename):
    try:
        with open(filename) as f:
            return True
            close(filename)
    except IOError:
        return False
        
def getScaleArchitectureOS():
    return getValue('scaleArchitectureOS')

def getValue(key):
    answer = None
    notFound = True
    try:
        #Loop through configc.dat first for overrides
        if file_exists(configCDATFileLocation):
            try:
                configFile = open(configCDATFileLocation, "r")
                notFound = True
                for line in configFile:
                    if notFound == False:
                        break
                    else:
                        line = string.strip(line)
                        if not line.startswith('#'):
                            keyValuePair = line.split('=')
                            key = key.rstrip()
                            key = key.lstrip()
                            if keyValuePair[0] == key:
                                valueSplitUp = keyValuePair[1].split('#')
                                valueSplitUp[0] = valueSplitUp[0].rstrip()
                                answer = valueSplitUp[0].lstrip()
                                notFound = False
                configFile.close()
            except:
                pass
        if notFound and file_exists(configDATFileLocation): 
            try:
                # Loop through config_linux.dat or config.dat file if not found in configc.dat file
                configFile = open(configDATFileLocation, "r")
                notFound = True
                for line in configFile:
                    if notFound == False:
                        break
                    else:
                        line = string.strip(line)
                        if not line.startswith('#'):
                            keyValuePair = line.split('=')
                            key = key.rstrip()
                            key = key.lstrip()
                            if keyValuePair[0] == key:
                                valueSplitUp = keyValuePair[1].split('#')
                                valueSplitUp[0] = valueSplitUp[0].rstrip()
                                answer = valueSplitUp[0].lstrip()
                                notFound = False
                configFile.close()
            except:
                pass
    except IOError:
        notFound = True
    
    return answer

