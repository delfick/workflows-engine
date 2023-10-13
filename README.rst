Computations
============

This is a playground repo for an idea for an api for a computations engine.


Changelog
---------

0.1.0 - TBD
    * TBD

Local development
-----------------

All these commands will bootstrap themselves such that a virtualenv and all
dependencies will exist where this is cloned and be isolated from the rest of
the machine.

Tests::

  > ./test.sh

  OR

  > ./run.sh tests

Lint and formatting::

  > ./lint
  > ./format
  > ./types

  OR

  > ./run.sh lint
  > ./run.sh format
  > ./run.sh types

For formatter and linters to work in your editor on the tests, it's recommended
you start your editor in an environment after doing::

  > source ./run.sh activate

Searching code::

  > ./find "something to look for"
