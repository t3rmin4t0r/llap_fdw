all:
	echo "make deps install"

install:
	python setup.py bdist_rpm
	sudo yum -y reinstall dist/llap_fdw-0.1.0-1.noarch.rpm 
deps:
	which rpmbuild || sudo yum install rpm-build
	rpmbuild --rebuild srpms/*.rpm
	sudo yum install ~/rpmbuild/RPMS/noarch/python27-PyHive-0.1.5.dev0-1.noarch.rpm \
	~/rpmbuild/RPMS/noarch/python27-thrift-sasl-0.1.0-1.noarch.rpm \
	~/rpmbuild/RPMS/x86_64/multicorn95-1.3.1-1.el7.centos.x86_64.rpm \
	~/rpmbuild/RPMS/x86_64/multicorn95-debuginfo-1.3.1-1.el7.centos.x86_64.rpm \
	~/rpmbuild/RPMS/x86_64/python27-sasl-0.1.3-1.x86_64.rpm \
	~/rpmbuild/RPMS/x86_64/python27-sasl-debuginfo-0.1.3-1.x86_64.rpm
