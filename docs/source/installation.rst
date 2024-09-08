==================
Installation guide
==================


Basic Installation:
###################

.. code-block:: console

   pip install django-valkey

|
|

Install with C-bindings for maximum performance:
################################################

.. code-block:: console

   pip install django-valkey[libvalkey]

|
|

Install with 3rd-party serializers
##################################


.. _msgpack:

Install with msgpack serializer:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   pip install django-valkey[msgpack]

|
|


Install with 3rd party compression libraries:
#############################################

lz4 library:
^^^^^^^^^^^^

.. code-block:: console

   pip install django-valkey[lz4]

pyzstd library:
^^^^^^^^^^^^^^^

.. code-block::

   pip install django-valkey[pyzstd]

brotli library:
^^^^^^^^^^^^^^^

.. code-block:: console

   pip install django-valkey[brotli]

|
|
|

Coming from django-redis?
#########################

check out our migration guide :doc:`migration`