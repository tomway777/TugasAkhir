#!"E:\Materi Kuliah\TA\Aplikasi\venv\Scripts\python.exe"
# EASY-INSTALL-ENTRY-SCRIPT: 'geomet==0.2.0.post2','console_scripts','geomet'
__requires__ = 'geomet==0.2.0.post2'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('geomet==0.2.0.post2', 'console_scripts', 'geomet')()
    )
