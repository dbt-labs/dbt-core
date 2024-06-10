# Unit test README


### The Why
We need to ensure that we can go from objects to dictionaries and back without any
changes. If some property or property value of an object gets dropped, added, or modified
while transitioning between its different possible representations, that is problematic.

### The How 
The easiest way to ensure things don't get droped, added, or modified is by starting
with an object, dictifying it, moving back to an object, and then asserting that everything
is equivalent. There are many potential edge cases though: what about optional fields, what
about lists of things, and etc. To address this we use hypothesis, which will build multiple
versions of the object we're interested in testing, and run the different generated versions
of the object through the test. This gives us confidence that for any allowable configuration
of an object, state is not changed when moving back and forth betweeen the python object
version and the seralized version.

### The What

- We test concrete classes in the codebase and do not test abstract classes as they are implementation details. [reference](https://enterprisecraftsmanship.com/posts/how-to-unit-test-an-abstract-class/)
