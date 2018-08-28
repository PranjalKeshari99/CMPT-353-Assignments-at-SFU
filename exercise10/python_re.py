import re

def getdate(pathname):
	result = re.search("([0-9]{8}\-[0-9]{2})", pathname)

	return result.group(1)

pathname = 'file:///Users/philipleblanc/Projects/CMPT353/exercise10/pagecounts-0/pagecounts-20160801-120000'
date = getdate(pathname)
print(date)