Custom Type Example
===================

This is an example of using a custom type with PyMongo. The example
here is a bit contrived, but shows how to use a
`~.son_manipulator.SONManipulator` to manipulate
documents as they are saved or retrieved from MongoDB. More
specifically, it shows a couple different mechanisms for working with
custom datatypes in PyMongo.

Setup
-----

We'll start by getting a clean database to use for the example:

.. doctest::

  >>> from pymongo.mongo_client import MongoClient
  >>> client = MongoClient()
  >>> client.drop_database("custom_type_example")
  >>> db = client.custom_type_example

Since the purpose of the example is to demonstrate working with custom
types, we'll need a custom datatype to use. Here we define the aptly
named ``Custom`` class, which has a single method, ``x``:

.. doctest::

  >>> class Custom(object):
  ...   def __init__(self, x):
  ...     self.__x = x
  ...
  ...   def x(self):
  ...     return self.__x
  ...
  >>> foo = Custom(10)
  >>> foo.x()
  10

When we try to save an instance of ``Custom`` with PyMongo, we'll
get an `~.bson.errors.InvalidDocument` exception:

.. doctest::

  >>> db.test.insert({"custom": Custom(5)})
  Traceback (most recent call last):
  InvalidDocument: cannot convert value of type <class 'Custom'> to bson

Manual Encoding
---------------

One way to work around this is to manipulate our data into something
we *can* save with PyMongo. To do so we define two methods,
``encode_custom`` and ``decode_custom``:

.. doctest::

  >>> def encode_custom(custom):
  ...   return {"_type": "custom", "x": custom.x()}
  ...
  >>> def decode_custom(document):
  ...   assert document["_type"] == "custom"
  ...   return Custom(document["x"])
  ...

We can now manually encode and decode ``Custom`` instances and
use them with PyMongo:

.. doctest::

  >>> db.test.insert({"custom": encode_custom(Custom(5))})
  ObjectId('...')
  >>> db.test.find_one()
  {u'_id': ObjectId('...'), u'custom': {u'x': 5, u'_type': u'custom'}}
  >>> decode_custom(db.test.find_one()["custom"])
  <Custom object at ...>
  >>> decode_custom(db.test.find_one()["custom"]).x()
  5

Automatic Encoding and Decoding
-------------------------------

Needless to say, that was a little unwieldy. Let's make this a bit
more seamless by creating a new
`~.son_manipulator.SONManipulator`.
`~.son_manipulator.SONManipulator` instances allow you
to specify transformations to be applied automatically by PyMongo:

.. doctest::

  >>> from pymongo.son_manipulator import SONManipulator
  >>> class Transform(SONManipulator):
  ...   def transform_incoming(self, son, collection):
  ...     for (key, value) in son.items():
  ...       if isinstance(value, Custom):
  ...         son[key] = encode_custom(value)
  ...       elif isinstance(value, dict): # Make sure we recurse into sub-docs
  ...         son[key] = self.transform_incoming(value, collection)
  ...     return son
  ...
  ...   def transform_outgoing(self, son, collection):
  ...     for (key, value) in son.items():
  ...       if isinstance(value, dict):
  ...         if "_type" in value and value["_type"] == "custom":
  ...           son[key] = decode_custom(value)
  ...         else: # Again, make sure to recurse into sub-docs
  ...           son[key] = self.transform_outgoing(value, collection)
  ...     return son
  ...

Now we add our manipulator to the `~.database.Database`:

.. doctest::

  >>> db.add_son_manipulator(Transform())

After doing so we can save and restore ``Custom`` instances seamlessly:

.. doctest::

  >>> db.test.remove() # remove whatever has already been saved
  {...}
  >>> db.test.insert({"custom": Custom(5)})
  ObjectId('...')
  >>> db.test.find_one()
  {u'_id': ObjectId('...'), u'custom': <Custom object at ...>}
  >>> db.test.find_one()["custom"].x()
  5

If we get a new `~.database.Database` instance we'll
clear out the `~.son_manipulator.SONManipulator`
instance we added:

.. doctest::

  >>> db = client.custom_type_example

This allows us to see what was actually saved to the database:

.. doctest::

  >>> db.test.find_one()
  {u'_id': ObjectId('...'), u'custom': {u'x': 5, u'_type': u'custom'}}

which is the same format that we encode to with our
``encode_custom`` method!

Binary Encoding
---------------

We can take this one step further by encoding to binary, using a user
defined subtype. This allows us to identify what to decode without
resorting to tricks like the ``_type`` field used above.

We'll start by defining the methods ``to_binary`` and
``from_binary``, which convert ``Custom`` instances to and
from `~.bson.binary.Binary` instances:

.. note:: You could just pickle the instance and save that. What we do
   here is a little more lightweight.

.. doctest::

  >>> from bson.binary import Binary
  >>> def to_binary(custom):
  ...   return Binary(str(custom.x()), 128)
  ...
  >>> def from_binary(binary):
  ...   return Custom(int(binary))
  ...

Next we'll create another
`~.son_manipulator.SONManipulator`, this time using the
methods we just defined:

.. doctest::

  >>> class TransformToBinary(SONManipulator):
  ...   def transform_incoming(self, son, collection):
  ...     for (key, value) in son.items():
  ...       if isinstance(value, Custom):
  ...         son[key] = to_binary(value)
  ...       elif isinstance(value, dict):
  ...         son[key] = self.transform_incoming(value, collection)
  ...     return son
  ...
  ...   def transform_outgoing(self, son, collection):
  ...     for (key, value) in son.items():
  ...       if isinstance(value, Binary) and value.subtype == 128:
  ...         son[key] = from_binary(value)
  ...       elif isinstance(value, dict):
  ...         son[key] = self.transform_outgoing(value, collection)
  ...     return son
  ...

Now we'll empty the `~.database.Database` and add the
new manipulator:

.. doctest::

  >>> db.test.remove()
  {...}
  >>> db.add_son_manipulator(TransformToBinary())

After doing so we can save and restore ``Custom`` instances
seamlessly:

.. doctest::

  >>> db.test.insert({"custom": Custom(5)})
  ObjectId('...')
  >>> db.test.find_one()
  {u'_id': ObjectId('...'), u'custom': <Custom object at ...>}
  >>> db.test.find_one()["custom"].x()
  5

We can see what's actually being saved to the database (and verify
that it is using a `~.bson.binary.Binary` instance) by
clearing out the manipulators and repeating our
`~.collection.Collection.find_one`:

.. doctest::

  >>> db = client.custom_type_example
  >>> db.test.find_one()
  {u'_id': ObjectId('...'), u'custom': Binary('5', 128)}
