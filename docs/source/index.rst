.. django-valkey documentation master file, created by
   sphinx-quickstart on Fri Sep  6 23:14:07 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=============
django-valkey
=============

`valkey <https://valkey.io/>`_ is an open source (BSD) high-performance key/value database that supports a variety of workloads such as *caching*, message queues, and can act as a primary database.

django-valkey is a customizable valkey backend to be used as a caching database in django.
this project was initially a fork of the wonderful ``django-redis`` project.

django-valkey Features
######################

#. Uses native valkey-py url notation connection strings
#. Pluggable clients:
   #. Default Client
   #. Herd Client
   #. Sentinel Client
   #. Sharded Client
   #. or just plug in your own client
#. Pluggable serializers:
   #. Pickle Serializer
   #. Json Serializer
   #. msgpack serializer
   #. or plug in your own serializer
#. Pluggable compression:
   #. brotli compression
   #. bz2 compression (bzip2)
   #. gzip compression
   #. lz4 compression
   #. lzma compression
   #. zlib compression
   #. zstd compression
   #. plug in your own
#. Pluggable parsers
   #. Valkey's default parser
   #. plug in your own
#. Pluggable connection pool
   #. Valkey's default connection pool
   #. plug in your own
#. Comprehensive test suite
#. Supports infinite timeouts
#. Facilities for raw access to Valkey client/connection pool
#. Highly configurable (really, just look around)
#. Unix sockets supported by default


Requirements
############

- `Python`_ 3.10+
- `Django`_ 3.2.9+
- `valkey-py`_ 6.0.0+ (probably works on older versions too)
- `Valkey server`_ 7.2.6+ (probably works with older versions too)

.. _Python: https://www.python.org/downloads/
.. _Django: https://www.djangoproject.com/download/
.. _valkey-py: https://pypi.org/project/valkey/
.. _Valkey server: https://valkey.io/download

.. toctree::
   :maxdepth: 3
   :caption: Index:

   installation
   migration
   configure/configurations
   commands/commands
   changes
