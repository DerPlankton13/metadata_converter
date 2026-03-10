# Design Considerations

This document contains some thoughts behind the design of this package hoping to be helpful for people wanting to extend, fork or else modify and extend this package.


## Python Version

Choosing an adequate python version lives in the tension between using the newest and best and longest supported python version vs. compatibility with other (older) packages.

For starters I will choose 3.12 as start, as it provides most of the newest syntax and will be supported for quite some time. Depending on whether there will be dependency conflicts within the most relevant packages, this may change.


## Configuration

I expect the users to have very specific requirements for their conversion that cannot be guessed. Thus the tool needs to be easily configurable and possibly be able to be extended by plugins. I will be using pydantic-settings as it is part of the big pydantic library and will thus likely be supported for a long time.
