#!/bin/sh

branch=ceph-testing

# dmclock
git subtree push \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git $branch

# add other subtree pull commands here...
