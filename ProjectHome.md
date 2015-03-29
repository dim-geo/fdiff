A fuse filesystem which mirrors a directory and saves differences in a file. It's written in python and it's based on xmp.py from [fuse-python](http://fuse.sourceforge.net/wiki/index.php/FusePython) package. It requires fuse-python package and [binary diff](http://starship.python.net/crew/atuining/cx_bsdiff/index.html) package. A basic usage example would be: <br>
<code>fdiff.py /home/user/new_mirrored_directory/ -o root=/home/user/old_directory/ -o datastorage=/home/user/data_persistence</code> <br>
In this example contents of old_directory would be mirrored in new_mirrored_directory and every change in new_mirrored_directory would be saved in data_persistence file.<br>
<br>
Deletion of files works like this: If data storage file contains changes about the deleted file, changes will be deleted.<br>
<br>
features: changing contents and filenames of files, deletion of files, rename directories, moving files. <br>
<b>not</b> working: creation of files, links