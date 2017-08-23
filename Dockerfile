FROM centos:latest

RUN yum update -y && \
    yum install epel-release -y && \
    yum install python34-setuptools -y --nogpgcheck && \
    yum clean all

RUN mkdir /src
ADD get-pip.py /src

WORKDIR /src
RUN ls
RUN python3 get-pip.py
RUN pip3 install pymemcache
