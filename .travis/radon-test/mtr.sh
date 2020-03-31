#!/bin/bash

TEST_FILE_PATH="./t/"
RESULT_FILE_PATH="./r/"
SUFFIX=".result"

cd $TEST_FILE_PATH
for FILE in `ls *.test`
do
    echo "testing "$FILE
    basename=`basename $FILE .test`
    tmpFile="tmp_"$basename$SUFFIX
    resultFile="../r/"$basename$SUFFIX

    string=`cat $FILE | tr -s '\r'`

    #split by ";"
    IFS=";"
    split=";"
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
    diff $tmpFile $resultFile
    
    if [ $? -ne 0 ]
    then
        # FAIL
        echo "testing "$FILE" FAIL"
        exit 1
    else
    	# SUCCESS
        echo "testing "$FILE" SUCCESS"
        rm -f $tmpFile
    fi
done
