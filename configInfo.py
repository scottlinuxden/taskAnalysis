
import os
import string

if os.name == 'posix':
    configDATFileLocation = '/etc/config_linux.dat'
    configCDATFileLocation = '/etc/configc.dat'
elif os.name == 'nt':
    configDATFileLocation = 'C:\Program Files\config.dat'
    configCDATFileLocation = 'C:\Program Files\configc.dat'
    
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
        answer = "C:\\Progam Files\\"
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

