from .settings import *

print 'Switching to "other" database.'
DATABASES['default']['NAME'] = os.path.join(BASE_DIR, 'other.sqlite3')
DO_SYNC = False
