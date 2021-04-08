#!/bin/bash


if [[ "$*" == "pull" ]]; then
    git pull
else    
    if [[ "$*" == "" ]]; then
        mess="Minor Changes!"
    else
        mess="$*"
    fi
    git add .
    echo "Message: $mess"
    git commit -m "$mess"
    git push -u origin master
fi