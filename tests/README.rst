Running the test suite
----------------------

.. code-block:: bash

  # start valkey and a sentinel (uses docker with image valkey:latest)
  PRIMARY=$(tests/start_valkey.sh)
  SENTINEL=$(tests/start_valkey.sh --sentinel)

  # or just wait 5 - 10 seconds and most likely this would be the case
  tests/wait_for_valkey.sh $PRIMARY 6379
  tests/wait_for_valkey.sh $SENTINEL 26379

  # run the tests
  tox

  # shut down valkey
  for container in $PRIMARY $SENTINEL; do
    docker stop $container && docker rm $container
  done
