#!/bin/bash

TEST_FILE_PATH="./t/"
SUFFIX=".result"

cd $TEST_FILE_PATH
for FILE in $(ls *.test)
do
    echo "testing "$FILE
    # such as: basename ddl.test .test, we'll get basename=ddl
    basename=$(basename $FILE .test)
    tmpFile="tmp_"$basename$SUFFIX
    resultFile="../r/"$basename$SUFFIX

    string=$(< $FILE  tr -s '\r')
    # split by ";"
    IFS=";"
    for sqlWithoutSemi in ${string[@]}
    do
        if [ "$sqlWithoutSemi" ]
        then
            # record every sql executed to tmp file 
            echo $sqlWithoutSemi";" >> $tmpFile
            # execute and record result to tmp file
            mysql -uroot -P3308 -h127.0.0.1 -e "$sqlWithoutSemi" >> $tmpFile
        else
            echo "empty sql, skip and continue"
        fi
    done

    # do check if has diff
    diff $tmpFile $resultFile
    
    if [ $? -ne 0 ]
    then
        # FAIL and exit
        echo "testing" $FILE "FAIL"
        exit 1
    else
    	# SUCCESS
        echo "testing" $FILE "SUCCESS"
        rm -f $tmpFile
    fi
done
