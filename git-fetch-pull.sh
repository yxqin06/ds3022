#!/bin/bash

REPO='/home/ash/ds3022'

if [ -d "$REPO/.git" ]
then
	echo "Updating repo test"
	cd "$REPO"
	echo "Fetching"
	git fetch
	echo "Pulling"
	git pull
else
	echo "cannot find .git file"
fi
