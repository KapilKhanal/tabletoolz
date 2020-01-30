==========
tabletoolz
==========


.. image:: https://img.shields.io/pypi/v/tabletoolz.svg
        :target: https://pypi.python.org/pypi/tabletoolz

.. image:: https://img.shields.io/travis/KapilKhanal/tabletoolz.svg
        :target: https://travis-ci.org/KapilKhanal/tabletoolz

.. image:: https://readthedocs.org/projects/tabletoolz/badge/?version=latest
        :target: https://tabletoolz.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/KapilKhanal/tabletoolz/shield.svg
     :target: https://pyup.io/repos/github/KapilKhanal/tabletoolz/
     :alt: Updates



**Lazy Manipulation of tables in a** *dplyr* **fashion using similar data manipulation verbs**
<br>

*Select
'''It selects a column from the dataframe. A dataframe is represented as a list of dictionaries. Each row is a dictionary of column:value pair.'''
Functions Skeletons
..* dplyr style select function
....select(df,cols)
'''input1 : A List of Dictionaries where each dictionaries key is column and value representing value of that column
   input2 : Cols: List of column to select
   Output : List of dictionary with the cols(key) on the Cols list
   Should output a generator object'''

<br>

* Mutate * <br>
'''It mutates a existing row. if a new column name is provided then the new column will be added in same row as new_column:mutated_value of the row. one can also mutate existing row but tis will overwrite the existing value with mutated one.'''




* Free software: MIT license
* Documentation: https://tabletoolz.readthedocs.io.


Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage



