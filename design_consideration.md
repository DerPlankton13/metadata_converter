# Design Considerations

This document contains some thoughts behind the design of this package hoping to be helpful for people wanting to extend, fork or else modify and extend this package.


## Python Version

Choosing an adequate python version lives in the tension between using the newest and best and longest supported python version vs. compatibility with other (older) packages.

For starters I will choose 3.12 as start, as it provides most of the newest syntax and will be supported for quite some time. Depending on whether there will be dependency conflicts within the most relevant packages, this may change.
