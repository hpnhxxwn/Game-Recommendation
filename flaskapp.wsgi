import sys
import site

sys.path.insert(0, '/var/www/html/flaskapp')
sys.path.append('/usr/lib/spark/python')
sys.path.append('/home/ubuntu/anaconda2/bin/python')
#sys.path.append('/home/ubuntu/anaconda2/lib/python2.7/')
sys.path.append('/usr/lib/spark/python/lib')
sys.path.append('/usr/lib/spark/bin/')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.10.4-src.zip')
sys.path.append('/home/ubuntu/flaskapp')
# Remember original sys.path.
prev_sys_path = list(sys.path) 
# Add site-packages directory.
site.addsitedir("/home/ubuntu/anaconda2/lib/python2.7/site-packages/")
# Reorder sys.path so new directories at the front.
new_sys_path = [] 
for item in list(sys.path): 
    if item not in prev_sys_path: 
        new_sys_path.append(item) 
        sys.path.remove(item) 
        sys.path[:0] = new_sys_path 
sys.path.insert(0, 'D:/PythonProjects/apricot')
from flaskapp import app as application
