all:
	python setup.py bdist_rpm
	sudo yum -y reinstall dist/llap_fdw-0.1.0-1.noarch.rpm 
